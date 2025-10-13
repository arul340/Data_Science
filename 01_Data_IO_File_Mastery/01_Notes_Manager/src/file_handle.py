from pathlib import Path

def read_notes(file_path: Path):
    # Read notes from a file
    if not file_path.exists():
        return[]
    
    with open(file_path, "r", encoding="utf-8")  as file:
        return [line.strip() for line in file.readlines()]
    
def add_note(file_path: Path, note: str) :
    # Add note to file
    with open(file_path, "a", encoding="utf-8") as file:
        file.write(note + "\n")


def update_note(file_path: Path, index:int, new_note: str):
    # Update note based index and new note
    notes = read_notes(file_path)
    if 0 < index < len(notes):
        notes[index] = new_note
        with open(file_path, "w", encoding="utf-8") as file:
            for note in notes:
                file.write(note + "\n")
    else :
        print("Index out of range")


def delete_note(file_path: Path, index=int) :
    # Delete note based index
    notes = read_notes(file_path)
    if 0 < index < len(notes):
        del notes[index]
        with open(file_path, "w", encoding="utf-8") as file:
            for note in notes:
                file.write(note + "\n")
    else :
        print("Index out of range")

        