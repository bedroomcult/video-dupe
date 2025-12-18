import os
import imagehash
from PIL import Image
import subprocess
import json
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_thumbnail(video_path, output_path, timestamp):
    """
    Extracts a thumbnail from a video at a specific timestamp.
    Requires FFmpeg to be installed.
    """
    command = [
        "ffmpeg",
        "-ss", str(timestamp),
        "-i", video_path,
        "-vframes", "1",
        "-q:v", "2",
        output_path,
        "-y"
    ]
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def process_video_file(video_file, temp_dir, hash_size, seconds_to_extract):
    """
    Worker function to process a single video file for a thread.
    Returns the file path and its list of hashes, or None if an error occurs.
    """
    try:
        hashes = []
        for i, sec in enumerate(seconds_to_extract):
            temp_thumbnail = os.path.join(temp_dir, f"{os.path.basename(video_file)}_{i}.jpg")
            if not get_thumbnail(video_file, temp_thumbnail, sec):
                return None
            
            with Image.open(temp_thumbnail) as img:
                hashes.append(imagehash.dhash(img, hash_size=hash_size))
            os.remove(temp_thumbnail)

        return video_file, hashes
    except Exception as e:
        tqdm.write(f"Error processing {os.path.basename(video_file)}: {e}")
        return None

def find_duplicate_videos(directory, hash_size=8, threshold=5, process_subdirectories=False, num_threads=4, seconds_to_extract=None):
    """
    Scans a directory for duplicate videos using dHash with a progress bar and multithreading.
    """
    if not seconds_to_extract:
        seconds_to_extract = [5]

    video_extensions = ('.mp4', '.mkv', '.avi', '.mov', '.flv')
    hashes = {}  # Now stores a list of hashes for each file
    duplicates = {}
    temp_dir = os.path.join(directory, 'temp_thumbnails')

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    print("Scanning directory for video files...")
    video_files = []
    if process_subdirectories:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(video_extensions):
                    video_files.append(os.path.join(root, file))
    else:
        video_files = [
            os.path.join(directory, f) for f in os.listdir(directory)
            if f.lower().endswith(video_extensions)
        ]

    print(f"Found {len(video_files)} videos. Starting analysis with {num_threads} threads...")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_video = {executor.submit(process_video_file, file, temp_dir, hash_size, seconds_to_extract): file for file in video_files}
        
        for future in tqdm(as_completed(future_to_video), total=len(video_files), desc="Hashing Videos", unit="file"):
            result = future.result()
            if result:
                video_file, video_hashes = result

                is_duplicate = False
                for existing_path, existing_hashes in hashes.items():
                    # Compare each hash from the list
                    total_distance = 0
                    for i in range(len(video_hashes)):
                        total_distance += video_hashes[i] - existing_hashes[i]
                    
                    # Calculate average distance
                    average_distance = total_distance / len(video_hashes)
                    
                    if average_distance <= threshold:
                        total_bits = hash_size * hash_size
                        match_percentage = (1 - (average_distance / total_bits)) * 100
                        
                        if existing_path not in duplicates:
                            duplicates[existing_path] = []
                        duplicates[existing_path].append({
                            'path': video_file,
                            'match_percentage': match_percentage
                        })
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    hashes[video_file] = video_hashes

    if os.path.exists(temp_dir) and not os.listdir(temp_dir):
        os.rmdir(temp_dir)
    
    return duplicates

def format_file_size(size_bytes):
    """
    Format file size in human readable format
    """
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s}{size_names[i]}"


def delete_duplicate_videos_from_json(json_file='duplicate_videos.json', min_match_percentage=90.0):
    """
    Deletes duplicate videos based on minimum match percentage threshold,
    loading the duplicates from the JSON file.
    Keeps the original and deletes the duplicates.
    """
    try:
        with open(json_file, 'r') as f:
            duplicates = json.load(f)
    except FileNotFoundError:
        print(f"Error: {json_file} not found. Run the duplicate detection first.")
        return 0
    except json.JSONDecodeError:
        print(f"Error: {json_file} is not a valid JSON file.")
        return 0

    # Calculate total size to be freed and collect files to delete
    files_to_delete = []
    total_size_to_free = 0

    for original, duplicates_list in duplicates.items():
        original_display = os.path.basename(original)

        for dup in duplicates_list:
            if dup['match_percentage'] >= min_match_percentage:
                dup_path = dup['path']
                dup_display = os.path.basename(dup_path)

                try:
                    file_size = os.path.getsize(dup_path)
                    files_to_delete.append((dup_path, dup_display, dup['match_percentage'], file_size))
                    total_size_to_free += file_size
                except OSError as e:
                    print(f"  - ERROR getting size for {dup_display}: {e}")

    if not files_to_delete:
        print("No files to delete based on the match percentage threshold.")
        return 0

    print(f"\nFiles to be deleted ({len(files_to_delete)} files):")
    for dup_path, dup_display, match_percentage, file_size in files_to_delete:
        print(f"  - {dup_display} (Match: {match_percentage:.2f}%, Size: {format_file_size(file_size)})")

    print(f"\nTotal storage space to be freed: {format_file_size(total_size_to_free)}")

    confirmation = input(f"\nAre you sure you want to delete {len(files_to_delete)} duplicate files to free {format_file_size(total_size_to_free)} of space? (yes/no): ")
    if confirmation.lower() not in ['yes', 'y', 'ye']:
        print("Deletion cancelled.")
        return 0

    # Proceed with deletion
    deleted_count = 0
    for dup_path, dup_display, match_percentage, file_size in files_to_delete:
        try:
            os.remove(dup_path)
            print(f"  - DELETED: {dup_display} (Match: {match_percentage:.2f}%, Size: {format_file_size(file_size)})")
            deleted_count += 1
        except OSError as e:
            print(f"  - ERROR deleting {dup_display}: {e}")

    print(f"\nTotal videos deleted: {deleted_count}")
    return deleted_count


def delete_duplicate_videos_direct(duplicates, min_match_percentage, include_subdirs=False):
    """
    Deletes duplicate videos based on minimum match percentage threshold.
    Keeps the original and deletes the duplicates.
    """
    # Calculate total size to be freed and collect files to delete
    files_to_delete = []
    total_size_to_free = 0

    for original, duplicates_list in duplicates.items():
        original_display = original if include_subdirs else os.path.basename(original)
        print(f"\nProcessing duplicates for original: {original_display}")

        for dup in duplicates_list:
            if dup['match_percentage'] >= min_match_percentage:
                dup_path = dup['path']
                dup_display = dup_path if include_subdirs else os.path.basename(dup_path)

                try:
                    file_size = os.path.getsize(dup_path)
                    files_to_delete.append((dup_path, dup_display, dup['match_percentage'], file_size))
                    total_size_to_free += file_size
                except OSError as e:
                    print(f"  - ERROR getting size for {dup_display}: {e}")
            else:
                dup_display = dup['path'] if include_subdirs else os.path.basename(dup['path'])
                print(f"  - SKIPPED: {dup_display} (Match: {dup['match_percentage']:.2f}% < {min_match_percentage}%)")

    if not files_to_delete:
        print("No files to delete based on the match percentage threshold.")
        return 0

    print(f"\nFiles to be deleted ({len(files_to_delete)} files):")
    for dup_path, dup_display, match_percentage, file_size in files_to_delete:
        print(f"  - {dup_display} (Match: {match_percentage:.2f}%, Size: {format_file_size(file_size)})")

    print(f"\nTotal storage space to be freed: {format_file_size(total_size_to_free)}")

    confirmation = input(f"\nAre you sure you want to delete {len(files_to_delete)} duplicate files to free {format_file_size(total_size_to_free)} of space? (yes/no): ")
    if confirmation.lower() not in ['yes', 'y', 'ye']:
        print("Deletion cancelled.")
        return 0

    # Proceed with deletion
    deleted_count = 0
    for dup_path, dup_display, match_percentage, file_size in files_to_delete:
        try:
            os.remove(dup_path)
            print(f"  - DELETED: {dup_display} (Match: {match_percentage:.2f}%, Size: {format_file_size(file_size)})")
            deleted_count += 1
        except OSError as e:
            print(f"  - ERROR deleting {dup_display}: {e}")

    print(f"\nTotal videos deleted: {deleted_count}")
    return deleted_count

def parse_seconds(s):
    """Parses a comma-separated string of seconds into a list of integers."""
    try:
        return [int(x) for x in s.split(',')]
    except (ValueError, IndexError):
        raise argparse.ArgumentTypeError("Seconds must be a comma-separated list of integers.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find duplicate videos in a directory.")
    parser.add_argument("-d", "--directory", required=True, help="The directory path to scan for videos.")
    parser.add_argument("-s", "--hash-size", type=int, default=8, help="Hash size (power of 2) for dHash. (default: 8)")
    parser.add_argument("-t", "--threshold", type=int, default=5, help="Hamming distance threshold for considering a match. Lower is more strict. (default: 5)")
    parser.add_argument("--sub", action="store_true", help="Include subdirectories in the scan.")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads to use for processing. (default: 4)")
    parser.add_argument("--sec", type=parse_seconds, default=[5], help="Comma-separated list of seconds to extract thumbnails. Example: 5,30 (default: 5)")
    parser.add_argument("--delete", action="store_true", help="Delete duplicate videos after finding them")
    parser.add_argument("--delete-from-json", action="store_true", help="Delete duplicates based on duplicate_videos.json file")
    parser.add_argument("--min-match", type=float, default=90.0, help="Minimum match percentage to consider a duplicate for deletion (default: 90.0)")
    args = parser.parse_args()
    
    video_dir = args.directory

    try:
        subprocess.run(["ffmpeg", "-version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: FFmpeg is not installed. Please install it using 'pkg install ffmpeg'.")
        exit()

    # If --delete-from-json flag is provided, just delete from the JSON file
    if args.delete_from_json:
        delete_duplicate_videos_from_json(min_match_percentage=args.min_match)
    elif not os.path.isdir(video_dir):
        print(f"Error: The provided path '{video_dir}' is not a valid directory.")
    else:
        found_duplicates = find_duplicate_videos(video_dir, hash_size=args.hash_size, threshold=args.threshold, process_subdirectories=args.sub, num_threads=args.threads, seconds_to_extract=args.sec)

        if found_duplicates:
            print("\n--- Duplicate Videos Found ---")
            for original, duplicates_list in found_duplicates.items():
                original_display = original if args.sub else os.path.basename(original)
                print(f"\nOriginal: {original_display}")
                for dup in duplicates_list:
                    dup_display = dup['path'] if args.sub else os.path.basename(dup['path'])
                    print(f"  - Duplicate: {dup_display} (Match: {dup['match_percentage']:.2f}%)")

            with open('duplicate_videos.json', 'w') as f:
                json.dump(found_duplicates, f, indent=4)
            print("\nResults saved to 'duplicate_videos.json'")

            if args.delete:
                print(f"\n--- Deleting duplicates with match percentage >= {args.min_match}% ---")
                delete_duplicate_videos_direct(found_duplicates, args.min_match, args.sub)
        else:
            print("\nNo duplicate videos found.")
