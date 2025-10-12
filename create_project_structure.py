from pathlib import Path

# === Base folder (semua mini-project) ===
base_dir = Path("01_Data_IO_File_Mastery")

# === Daftar mini project ===
projects = {
    "01_Notes_Manager": "notes_manager.py",
    "02_Sales_Summary": "sales_summary.py",
    "03_Product_Export_Manager": "product_export_manager.py",
    "04_Excel_Data_Integrator": "excel_data_integrator.py",
    "05_Daily_Data_Pipeline_Monitor": "daily_data_pipeline_monitor.py",
}

# === Loop untuk tiap mini project ===
for folder, main_script in projects.items():
    project_path = base_dir / folder

    # Buat subfolder
    for subfolder in ["data/raw", "data/processed", "logs", "src"]:
        (project_path / subfolder).mkdir(parents=True, exist_ok=True)

    # Buat file utama di src/
    main_file = project_path / "src" / main_script
    if not main_file.exists():
        main_file.write_text(f"# {main_script.replace('_', ' ').title()}\n\n# Start coding here...\n")

    # Buat file tambahan di src/
    for extra in ["file_handler.py", "utils.py", "__init__.py"]:
        extra_file = project_path / "src" / extra
        if not extra_file.exists():
            extra_file.write_text(f"# {extra.replace('_', ' ').title()} module\n")

    # Buat README.md di project
    readme = project_path / "README.md"
    if not readme.exists():
        readme.write_text(f"# {folder.replace('_', ' ')}\n\nDescription will be written here.\n")

print("✅ All project folders and files created successfully!")
