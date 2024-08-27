import csv
import os
import json
import argparse

def load_csv_data(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def get_expected_size(file_info):
    return float(file_info['filesize'].split()[0]) * 1024 * 1024  # Convert MB to bytes

def get_file_status(actual_size, expected_size, tolerance_percent=1):
    if abs(actual_size - expected_size) <= expected_size * (tolerance_percent / 100):
        return 'completed'
    elif actual_size > 0:
        return 'partial'
    else:
        return 'incomplete'

def rebuild_metadata(csv_file, base_download_dir):
    csv_data = load_csv_data(csv_file)
    
    file_info_dict = {}
    for file_info in csv_data:
        key = (file_info['model'], file_info['scenario'], file_info['variable'], file_info['filename'])
        file_info_dict[key] = file_info

    for root, dirs, files in os.walk(base_download_dir):
        if 'r1i1p1f1' in root:
            parts = root.split(os.sep)
            model = parts[-4]
            scenario = parts[-3]
            variable = parts[-1]
            
            metadata = {}
            metadata_path = os.path.join(root, 'download_metadata.json')
            
            for filename in files:
                if filename == 'download_metadata.json':
                    continue
                
                key = (model, scenario, variable, filename)
                if key in file_info_dict:
                    file_info = file_info_dict[key]
                    expected_size = get_expected_size(file_info)
                    file_path = os.path.join(root, filename)
                    actual_size = os.path.getsize(file_path)
                    
                    status = get_file_status(actual_size, expected_size)
                    metadata[filename] = {'status': status, 'size': actual_size}
                    
                    print(f"File: {filename}")
                    print(f"  Expected size: {expected_size:.2f} bytes")
                    print(f"  Actual size: {actual_size} bytes")
                    print(f"  Status: {status}")
                else:
                    print(f"Warning: File not found in CSV: {os.path.join(root, filename)}")
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"Updated metadata for: {root}")
            print("-----------------------------")

def main():
    parser = argparse.ArgumentParser(description="Rebuild all download_metadata.json files based on CSV and existing downloads.")
    parser.add_argument("csv_file", help="Path to the CSV file containing file information.")
    parser.add_argument("base_download_dir", help="Base directory where files are downloaded.")
    parser.add_argument("--tolerance", type=float, default=1.0, help="Tolerance percentage for file size matching (default: 1.0)")
    
    args = parser.parse_args()
    
    rebuild_metadata(args.csv_file, args.base_download_dir)
    print("Metadata rebuild process completed for all directories.")

if __name__ == "__main__":
    main()