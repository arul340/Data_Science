from pathlib import Path

# ==== Folder Structure ====
base_dir = Path("01_Data_IO_File_Mastery")

structure = {
    "01_Notes_Manager": "notes_manager.py",
    "02_Sales_Summary": "sales_summary.py",
    "03_Product_Export_Manager": "product_export_manager.py",
    "04_Excel_Data_Integrator": "excel_data_integrator.py",
    "05_Daily_Data_Pipeline_Monitor": "daily_data_pipeline_monitor.py",
}

# ==== Create Folder Structure in Base Directory ====
for folder, file_name in structure.items():
    folder_path = base_dir / folder
    folder_path.mkdir(parents=True, exist_ok=True)

    # === Create main Python file ===
    file_path = folder_path / file_name
    if not file_path.exists():
        file_path.write_text("# " + file_name.replace("_", " ").title() + "\n\n# Start coding here...\n")
        print(f"✅ Created file: {file_path}")
    else:
        print(f"⚠️ File already exists: {file_path}")

    # === Create README.md for each project ===
    readme_path = folder_path / "README.md"
    if not readme_path.exists():
        readme_path.write_text(f"# {folder.replace('_', ' ')}\n\nDescription all of project will write here.\n")
        print(f"📄 Created README.md at {readme_path}")
    else:
        print(f"⚠️ README.md already exists in {folder}")

# === Create README.md in the base directory ===
base_readme = base_dir / "README.md"
if not base_readme.exists():
    base_readme.write_text("# 01_Data_IO_File_Mastery\n\nDescription all of project will write here.")
    print(f"✅ Created main README.md in {base_dir}")
else:
    print("⚠️ Main README.md already exists.")

print("\n🎉 Folder structure created successfully.")
