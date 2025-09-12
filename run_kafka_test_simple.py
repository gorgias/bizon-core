#!/usr/bin/env python3
"""
Simple Local Kafka Test Runner

Uses the existing kafka_async_test_setup to run the E2E test locally.
This replicates the failing CI test in your local environment.

Usage:
    python run_kafka_test_simple.py [--no-cleanup] [--verbose]
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Pat


def log(message: str, level: str = "INFO"):
    """Simple logging"""
    icons = {"INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "❌", "WARNING": "⚠️"}
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {icons.get(level, '📝')} {message}")


def run_command(cmd: str, cwd: str = None, timeout: int = 60) -> tuple:
    """Run command and return (exit_code, output)"""
    try:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return 1, f"Command timed out after {timeout} seconds"
    except Exception as e:
        return 1, str(e)


def check_docker():
    """Check if Docker is running"""
    exit_code, _ = run_command("docker info", timeout=10)
    return exit_code == 0


def wait_for_kafka(test_dir: Path, max_attempts: int = 24) -> bool:
    """Wait for Kafka to be ready"""
    log("Waiting for Kafka to be ready...")

    for attempt in range(1, max_attempts + 1):
        exit_code, _ = run_command(
            "docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list",
            cwd=str(test_dir),
            timeout=10,
        )

        if exit_code == 0:
            log("Kafka is ready!", "SUCCESS")
            return True

        log(f"Waiting for Kafka... (attempt {attempt}/{max_attempts})")
        time.sleep(5)

    log("Kafka failed to start within timeout", "ERROR")
    return False


def main():
    parser = argparse.ArgumentParser(description="Simple Local Kafka Test Runner")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't cleanup containers after test")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Check we're in the right directory
    if not Path("tests/e2e/kafka_async_test_setup").exists():
        log("Please run this script from the bizon-core root directory", "ERROR")
        return 1

    # Check Docker
    if not check_docker():
        log("Docker is not running. Please start Docker first.", "ERROR")
        return 1

    test_dir = Path("tests/e2e/kafka_async_test_setup")

    try:
        print("🚀 Starting Local Kafka E2E Test")
        print("=" * 40)

        # Cleanup any existing containers
        log("Cleaning up any existing containers...")
        run_command("docker-compose down -v", cwd=str(test_dir))

        # Start Kafka cluster
        log("Starting Kafka cluster...")
        exit_code, output = run_command("docker-compose up -d", cwd=str(test_dir))
        if exit_code != 0:
            log(f"Failed to start Kafka: {output}", "ERROR")
            return 1

        # Wait for Kafka to be ready
        if not wait_for_kafka(test_dir):
            log("Showing Kafka logs for debugging:", "ERROR")
            _, logs = run_command("docker-compose logs kafka", cwd=str(test_dir))
            print(logs)
            return 1

        # Start test producer briefly to generate some data
        log("Starting test data producer...")
        run_command("docker-compose up -d test-producer", cwd=str(test_dir))

        # Let it produce data for a bit
        log("Generating test data...")
        time.sleep(15)

        # Stop producer
        run_command("docker-compose stop test-producer", cwd=str(test_dir))

        # Clean up any output files from previous runs
        for file in ["high_volume.json", "low_volume.json", "kafka_test.json"]:
            if Path(file).exists():
                os.remove(file)

        # Run the actual test
        log("Running Kafka async polling E2E test...")

        test_cmd = (
            "python -m pytest "
            "tests/e2e/test_e2e_kafka_async_polling.py::TestKafkaAsyncPollingE2E::test_async_polling_prevents_starvation "
            f"-v {'--tb=short' if not args.verbose else '-s'}"
        )

        exit_code, output = run_command(test_cmd, timeout=300)  # 5 minute timeout

        if args.verbose:
            print(output)

        if exit_code == 0:
            log("Test passed! Async polling prevented topic starvation.", "SUCCESS")
            print("\n📊 Test Results Summary:")
            print("=" * 30)
            print("✅ Kafka cluster started successfully")
            print("✅ Test data produced (high and low volume topics)")
            print("✅ Async polling prevented low-volume topic starvation")
            print("✅ File outputs generated and validated")
            print("\n🎯 The async polling implementation is working correctly!")
            print(f"\n💡 Kafka UI available at: http://localhost:8080")

            return 0
        else:
            log("Test failed!", "ERROR")
            print(f"\nTest output:\n{output}")
            return 1

    except KeyboardInterrupt:
        log("Test interrupted by user", "WARNING")
        return 1
    except Exception as e:
        log(f"Unexpected error: {e}", "ERROR")
        return 1

    finally:
        if not args.no_cleanup:
            log("Cleaning up containers...")
            run_command("docker-compose down -v", cwd=str(test_dir))

            # Clean up output files
            for file in ["high_volume.json", "low_volume.json", "kafka_test.json"]:
                if Path(file).exists():
                    os.remove(file)

            log("Cleanup completed", "SUCCESS")
        else:
            log("Skipping cleanup (--no-cleanup specified)")
            log("To cleanup later, run: cd tests/e2e/kafka_async_test_setup && docker-compose down -v")


if __name__ == "__main__":
    sys.exit(main())
