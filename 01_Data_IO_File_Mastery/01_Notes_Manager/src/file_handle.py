def add_note (notes_path, note):
    with open(notes_path, "a", encoding="utf-8") as file:
        file.write(note + "\n") 