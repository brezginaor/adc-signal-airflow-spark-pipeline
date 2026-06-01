from pathlib import Path


STATE_FILE = Path("/tmp/adc_signal_unstable_task_state.txt")
MAX_FAILED_ATTEMPTS = 2


def main():
    if not STATE_FILE.exists():
        STATE_FILE.write_text("1")
        raise RuntimeError("Intentional failure: signal quality service is temporarily unavailable")

    attempt = int(STATE_FILE.read_text())

    if attempt < MAX_FAILED_ATTEMPTS:
        STATE_FILE.write_text(str(attempt + 1))
        raise RuntimeError(f"Intentional failure on attempt {attempt + 1}")

    print("Unstable signal quality check passed after retries")
    STATE_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()