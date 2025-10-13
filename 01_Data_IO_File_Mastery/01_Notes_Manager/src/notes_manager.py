# Notes Manager.Py

# Start coding here...
from pathlib import Path
from src.file_handle import read_notes, add_note, delete_note, update_note
from src.utils import log_action

# === Path Setup ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data"/"raw"/"notes.txt"
LOG_PATH = BASE_DIR / "logs" / "app.log"

DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# === Main Function ===
while True :
    print("=== Notes Manager ===")
    print("1. Add Note")
    print("2. Show All Notes")
    print("3. Edit Note")
    print("4. Delete Note")
    print("0. Exit")

    choice  = input("Enter your choice: ")
    if choice == "1":
        note = input("Enter your note: ")
        if note.strip():
            add_note(DATA_PATH, note)
            log_action(LOG_PATH, f"Tambah catatan: {note}")
            print("Note added successfully!")
        else :
            print("Note cannot be empty")

    elif choice == "2":
        notes = read_notes(DATA_PATH)
        if notes :
            print("\n=== Notes List===")
            for index, note in enumerate(notes, start=1):
                print(f"{index}. {note}")
        
    elif choice == "3":
        notes = read_notes(DATA_PATH)
        if not notes:
            print("No notes found.")
            continue
        for index, note in enumerate(notes, start=1):
            print(f"{index}. {note}")
        try:
            note_index = int(input("Enter the note index to edit: "))
            new_note = input("Enter the new note: ")
            update_note(DATA_PATH, note_index, new_note)
            log_action(LOG_PATH, f"Update catatan: #{note_index+1}: {new_note}")
            print ("Note updated successfully!")
        except ValueError:
            print("Invalid input. Please enter a valid note index.")

    elif choice == "4":
        notes = read_notes(DATA_PATH)
        if not notes:
            print("No notes found.")
            continue
        for index, note in enumerate(notes, start=1):
            print(f"{index}. {note}")
        try:
            note_index = int(input("Enter the note index to delete: "))
            delete_note(DATA_PATH, note_index)
            log_action(LOG_PATH, f"Delete note: #{note_index+1}")
        except ValueError:
            print("Invalid input. Please enter a valid note index.")

    elif choice == "0":
        print("Good bye! Have a nice day!")
        log_action(LOG_PATH, "Aplication closed by user")
        break

    else :
        print("Invalid choice. Please try again.")
