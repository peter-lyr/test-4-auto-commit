import os
import random
import shutil
import multiprocessing
import time
from functools import partial


def generate_single_file(file_info, progress_queue):
    """生成单个文件的函数，带有实时进度报告"""
    filename, file_size = file_info

    # 使用优化的缓冲区大小 - 512KB
    buffer_size = 512 * 1024  # 512KB缓冲区

    # 获取进程ID
    pid = os.getpid()

    # 发送开始消息
    progress_queue.put(
        {
            "pid": pid,
            "filename": filename,
            "status": "started",
            "file_size": file_size,
            "message": f"进程 {pid:>5} 开始生成文件 {filename:<10} ({file_size/(1024*1024):6.2f} MB)...",
        }
    )

    start_time = time.time()
    last_report_time = start_time

    try:
        with open(filename, "wb") as f:
            bytes_written = 0

            while bytes_written < file_size:
                remaining = file_size - bytes_written
                current_buffer_size = min(buffer_size, remaining)

                # 使用优化的随机字节生成方法
                # 方法1: 使用os.urandom (最快的方法)
                try:
                    random_bytes = os.urandom(current_buffer_size)
                except:
                    # 方法2: 使用预先生成的随机块
                    # 创建一个预生成的随机块池，避免重复生成
                    random_bytes = bytes(
                        random.randint(0, 255) for _ in range(int(current_buffer_size))
                    )

                # 写入文件
                f.write(random_bytes)
                bytes_written += current_buffer_size

                # 定期报告进度（每5秒最多一次）
                current_time = time.time()
                if current_time - last_report_time >= 5.0:  # 每5秒报告一次
                    progress = (bytes_written / file_size) * 100
                    elapsed = current_time - start_time
                    if elapsed > 0:
                        speed = bytes_written / (1024 * 1024) / elapsed  # MB/s
                    else:
                        speed = 0

                    progress_queue.put(
                        {
                            "pid": pid,
                            "filename": filename,
                            "status": "progress",
                            "progress": progress,
                            "bytes_written": bytes_written,
                            "file_size": file_size,
                            "speed": speed,
                            "elapsed": elapsed,
                        }
                    )
                    last_report_time = current_time

        end_time = time.time()
        elapsed = end_time - start_time
        speed = 9999
        if elapsed:
            speed = file_size / (1024 * 1024) / elapsed  # MB/s

        # 发送完成消息
        progress_queue.put(
            {
                "pid": pid,
                "filename": filename,
                "status": "completed",
                "message": f"进程 {pid:>5} 完成 {filename:<10} - 耗时: {elapsed:6.2f}秒, 速度: {speed:5.2f} MB/s",
                "elapsed": elapsed,
                "speed": speed,
            }
        )

        return filename, file_size

    except Exception as e:
        # 发送错误消息
        progress_queue.put(
            {
                "pid": pid,
                "filename": filename,
                "status": "error",
                "message": f"进程 {pid:>5} 生成文件 {filename:<10} 时出错: {str(e)}",
            }
        )
        raise


def progress_monitor(progress_queue, total_files, total_size_bytes):
    """进度监控器，在主进程中运行，显示所有子进程的进度"""
    # 存储每个进程的进度信息
    process_info = {}
    completed_files = 0
    completed_size = 0
    start_time = time.time()

    print("进度监控器已启动，显示所有子进程的实时进度...")
    print("=" * 100)

    while completed_files < total_files:
        try:
            # 从队列中获取进度信息（阻塞，最多等待1秒）
            info = progress_queue.get(timeout=1)
            pid = info["pid"]
            filename = info["filename"]

            if info["status"] == "started":
                print(f"[{time.strftime('%H:%M:%S')}] {info['message']}")
                # 确保从消息中获取文件大小
                file_size = info.get("file_size", 0)
                process_info[pid] = {
                    "filename": filename,
                    "progress": 0,
                    "bytes_written": 0,
                    "file_size": file_size,
                    "speed": 0,
                    "start_time": time.time(),
                }

            elif info["status"] == "progress":
                # 确保在更新进度信息时也更新文件大小
                file_size = info.get(
                    "file_size", process_info.get(pid, {}).get("file_size", 0)
                )
                if pid not in process_info:
                    process_info[pid] = {
                        "filename": filename,
                        "progress": 0,
                        "bytes_written": 0,
                        "file_size": file_size,
                        "speed": 0,
                        "start_time": time.time(),
                    }

                process_info[pid].update(
                    {
                        "progress": info["progress"],
                        "bytes_written": info["bytes_written"],
                        "file_size": file_size,  # 确保文件大小也被更新
                        "speed": info["speed"],
                        "last_update": time.time(),
                    }
                )

                # 显示这个进程的进度，使用固定宽度确保对齐
                file_info = process_info[pid]
                progress_str = f"{file_info['progress']:5.1f}%"
                written_str = f"{file_info['bytes_written']/(1024*1024):6.2f}"
                total_str = f"{file_info['file_size']/(1024*1024):6.2f}"
                speed_str = f"{file_info['speed']:5.2f}"

                print(
                    f"[{time.strftime('%H:%M:%S')}] 进程 {pid:>5}: {filename:<10} - "
                    f"进度: {progress_str} ({written_str}/{total_str} MB) - "
                    f"速度: {speed_str} MB/s"
                )

            elif info["status"] == "completed":
                print(f"[{time.strftime('%H:%M:%S')}] {info['message']}")
                if pid in process_info:
                    completed_files += 1
                    completed_size += process_info[pid]["file_size"]

                # 计算总体进度
                progress = (completed_size / total_size_bytes) * 100
                elapsed_time = time.time() - start_time
                if elapsed_time > 0:
                    overall_speed = (
                        completed_size / (1024 * 1024 * 1024) / (elapsed_time / 3600)
                    )  # GB/hour
                else:
                    overall_speed = 0

                print(
                    f"[{time.strftime('%H:%M:%S')}] 总体进度: {progress:5.1f}% "
                    f"({completed_size/(1024*1024*1024):6.2f} GB / {total_size_bytes/(1024*1024*1024):6.2f} GB) - "
                    f"速度: {overall_speed:6.2f} GB/小时 - "
                    f"已完成 {completed_files:4d}/{total_files:4d} 个文件"
                )
                print("-" * 100)

            elif info["status"] == "error":
                print(f"[{time.strftime('%H:%M:%S')}] {info['message']}")

        except:
            # 队列为空，继续等待
            pass

    total_time = time.time() - start_time
    print(f"所有文件生成完成! 总共生成了 {total_files:4d} 个文件")
    print(f"总大小: {completed_size/(1024*1024*1024):6.2f} GB")
    print(f"总耗时: {total_time:8.2f} 秒")
    print(
        f"平均速度: {completed_size/(1024*1024*1024) / (total_time/3600):6.2f} GB/小时"
    )


def generate_random_bin_files_parallel():
    """使用多进程生成多个文件"""
    # 总大小设置为102GB
    total_size_gb = 0.12
    total_size_bytes = total_size_gb * 1024 * 1024 * 1024  # 102GB in bytes

    # 每个文件大小范围（30MB到50MB）
    min_file_size = 0.1 * 1024 * 1024  # 30MB in bytes
    max_file_size = 1.1 * 1024 * 1024  # 50MB in bytes

    print(f"目标总大小: {total_size_gb:3f} GB")
    print(
        f"每个文件大小: {min_file_size/(1024*1024):5.1f} MB 到 {max_file_size/(1024*1024):5.1f} MB"
    )

    # 预计算所有文件的大小和名称
    file_tasks = []
    bytes_written_total = 0
    file_count = 0

    while bytes_written_total < total_size_bytes:
        # 为当前文件随机选择大小
        remaining_bytes = total_size_bytes - bytes_written_total

        # 如果剩余字节数小于最小文件大小，调整文件大小
        if remaining_bytes < min_file_size:
            current_file_size = remaining_bytes
        else:
            # 随机选择文件大小，但不超过剩余需要的大小
            max_possible = min(max_file_size, remaining_bytes)
            current_file_size = random.randint(int(min_file_size), int(max_possible))

        # 生成文件名，使用4位数字格式
        filename = f"d{file_count:04d}.bin"
        file_tasks.append((filename, current_file_size))

        bytes_written_total += current_file_size
        file_count += 1

    print(
        f"将生成 {file_count:4d} 个文件，总大小: {bytes_written_total/(1024*1024*1024):6.2f} GB"
    )

    # 使用更保守的进程数，避免内存问题
    # 使用CPU核心数，但不大于16
    num_processes = min(multiprocessing.cpu_count(), 16, len(file_tasks))
    print(f"使用 {num_processes:2d} 个进程并行生成文件...")

    # 创建Manager和共享队列
    manager = multiprocessing.Manager()
    progress_queue = manager.Queue()

    # 启动进度监控器线程
    from threading import Thread

    monitor_thread = Thread(
        target=progress_monitor,
        args=(progress_queue, len(file_tasks), total_size_bytes),
    )
    monitor_thread.daemon = True
    monitor_thread.start()

    # 给一点时间让监控线程启动
    time.sleep(1)

    # 使用多进程池处理任务，但限制同时运行的进程数
    start_time = time.time()

    try:
        # 使用更小的chunksize来更好地分配任务
        with multiprocessing.Pool(processes=num_processes) as pool:
            # 使用partial函数将progress_queue传递给generate_single_file
            worker_func = partial(generate_single_file, progress_queue=progress_queue)

            # 使用imap_unordered并行处理任务，使用更小的chunksize
            results = []
            for result in pool.imap_unordered(worker_func, file_tasks, chunksize=1):
                results.append(result)

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        import traceback

        traceback.print_exc()

    total_time = time.time() - start_time

    # 等待监控线程完成
    monitor_thread.join(timeout=5)

    # 关闭manager
    manager.shutdown()

    return file_count


def main():
    print("开始使用多进程生成102GB的随机二进制文件集合...")
    print("使用稳定版本：512KB缓冲区，优化的随机数生成，保守的进程数")

    # 检查磁盘空间
    try:
        total, used, free = shutil.disk_usage(".")
        required_space = 102 * 1024 * 1024 * 1024  # 102GB in bytes

        if free < required_space:
            print(f"警告: 磁盘空间不足!")
            print(f"需要: {required_space/(1024*1024*1024):6.2f} GB")
            print(f"可用: {free/(1024*1024*1024):6.2f} GB")
            response = input("是否继续? (y/n): ")
            if response.lower() != "y":
                print("操作已取消")
                return
    except Exception as e:
        print(f"无法检查磁盘空间: {e}")
        print("将继续执行，但请确保有足够的磁盘空间...")

    # 生成文件
    file_count = generate_random_bin_files_parallel()

    print(f"完成! 共生成 {file_count:4d} 个文件，总大小约102GB")


if __name__ == "__main__":
    # 在Windows上，multiprocessing需要这个保护
    main()
