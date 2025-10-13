from datetime import datetime

def log_action(log_path, message):
    # Log action to a file
    with open(log_path, "a", encoding="utf-8") as log:
        timestamp = datetime.now().strftime("%Y-%m-%d/%H:%M:%S")
        log.write(f"[{timestamp}] {message} \n")