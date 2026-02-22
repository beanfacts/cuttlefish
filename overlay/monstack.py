#!/usr/bin/env python3

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import yaml


class MonitoringStack:
    def __init__(self, listen_addr, command):
        self.listen_addr = listen_addr
        self.command = command
        self.target_process = None
        self.process_exporter = None
        self.config_file = None

    def create_process_exporter_config(self, pid, process_name):
        """Create process-exporter configuration file"""
        config = {
            "process_names": [
                {"name": f"{process_name}", "cmdline": self.command, "children": True}
            ]
        }

        # Create temporary config file
        fd, self.config_file = tempfile.mkstemp(
            suffix=".yaml", prefix="process-exporter-"
        )
        with os.fdopen(fd, "w") as f:
            yaml.dump(config, f, default_flow_style=False)

        print(f"Created process-exporter config: {self.config_file}")
        return self.config_file

    def start_target_process(self):
        """Start the target process"""
        print(f"Starting target process: {' '.join(self.command)}")
        try:
            self.target_process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid,  # Create new process group
            )
            print(f"Target process started with PID: {self.target_process.pid}")
            return self.target_process.pid
        except Exception as e:
            print(f"Failed to start target process: {e}")
            return None

    def start_process_exporter(self, config_file):
        """Start process-exporter with the generated config"""
        try:
            cmd = [
                "process-exporter",
                "--config.path",
                config_file,
                "--web.listen-address",
                self.listen_addr,
                "--procfs",
                "/proc",
                "--children",
                "true",
                "--threads",
                "true",
                "--smaps",
                "true",
                "--recheck",
            ]

            print(f"Starting process-exporter: {' '.join(cmd)}")
            self.process_exporter = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            print(f"Process-exporter started with PID: {self.process_exporter.pid}")
            print(f"Metrics available at: http://{self.listen_addr}/metrics")
            return True
        except Exception as e:
            print(f"Failed to start process-exporter: {e}")
            return False

    def monitor_target_process(self):
        """Monitor the target process and restart if needed"""
        while self.target_process and self.target_process.poll() is None:
            time.sleep(1)

        if self.target_process:
            return_code = self.target_process.poll()
            print(f"Target process exited with code: {return_code}")
            self.cleanup()
            sys.exit(return_code if return_code else 0)

    def signal_handler(self, signum, frame):
        """Handle SIGTERM and SIGINT"""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        """Clean up processes and temporary files"""
        print("Cleaning up...")

        # Stop target process
        if self.target_process and self.target_process.poll() is None:
            print("Terminating target process...")
            try:
                # Send SIGTERM to the process group
                os.killpg(os.getpgid(self.target_process.pid), signal.SIGTERM)
                # Wait for graceful shutdown
                self.target_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print("Target process didn't exit gracefully, forcing kill...")
                os.killpg(os.getpgid(self.target_process.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass  # Process already dead

        # Stop process-exporter
        if self.process_exporter and self.process_exporter.poll() is None:
            print("Terminating process-exporter...")
            self.process_exporter.terminate()
            try:
                self.process_exporter.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process_exporter.kill()

        # Remove temporary config file
        if self.config_file and os.path.exists(self.config_file):
            os.unlink(self.config_file)
            print(f"Removed config file: {self.config_file}")

    def run(self):
        """Main execution method"""
        # Set up signal handlers
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        try:
            # Start target process
            pid = self.start_target_process()
            if not pid:
                return 1

            # Wait a moment for the process to fully start
            time.sleep(2)

            # Create process-exporter config
            process_name = os.path.basename(self.command[0])
            config_file = self.create_process_exporter_config(pid, process_name)

            # Start process-exporter
            if not self.start_process_exporter(config_file):
                self.cleanup()
                return 1

            print("Monitoring stack is running. Press Ctrl+C to stop.")

            # Monitor target process in a separate thread
            monitor_thread = threading.Thread(target=self.monitor_target_process)
            monitor_thread.daemon = True
            monitor_thread.start()

            # Keep the main thread alive
            while True:
                time.sleep(1)

                # Check if processes are still alive
                if self.target_process and self.target_process.poll() is not None:
                    break

                if self.process_exporter and self.process_exporter.poll() is not None:
                    print("Process-exporter died unexpectedly")
                    break

        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Error: {e}")
            return 1
        finally:
            self.cleanup()

        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Start a program and monitor it with process-exporter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ./monstack.py 127.0.0.1:9256 ceph-osd -c ceph.conf -n osd.0
  ./monstack.py 0.0.0.0:9090 python my_app.py --config config.json
        """,
    )

    parser.add_argument(
        "listen_addr", help="Address for process-exporter to listen on (host:port)"
    )
    parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="Command to start and monitor"
    )

    args = parser.parse_args()

    if not args.command:
        print("Error: No command specified")
        parser.print_help()
        return 1

    # Validate listen address format
    if ":" not in args.listen_addr:
        print("Error: Listen address must be in format host:port")
        return 1

    try:
        host, port = args.listen_addr.split(":", 1)
        int(port)  # Validate port is numeric
    except ValueError:
        print("Error: Invalid listen address format. Use host:port")
        return 1

    print(f"MonitoringStack starting...")
    print(f"Listen address: {args.listen_addr}")
    print(f"Target command: {' '.join(args.command)}")

    stack = MonitoringStack(args.listen_addr, args.command)
    return stack.run()


if __name__ == "__main__":
    sys.exit(main())
