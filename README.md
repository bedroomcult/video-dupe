# Video Duplicate Finder

A Python script to find duplicate videos in a directory using perceptual hashing and FFmpeg for extracting video thumbnails. The script compares video files by extracting frames at specified time points and comparing their perceptual hashes.

## Features

- Find duplicate videos using dHash (difference hash) algorithm
- Extract thumbnails from specific timestamps in videos
- Multi-threaded processing for faster scanning
- Configurable hash size and match threshold
- Support for recursive directory scanning
- Option to delete duplicate files after detection
- Export results to JSON for later processing
- Size estimation and confirmation prompts before deletion

## Dependencies

- Python 3.x
- FFmpeg (for extracting video thumbnails)
- Required Python packages:
  - Pillow (PIL)
  - imagehash
  - tqdm
  - argparse

Install the required Python packages with pip:

```bash
pip install pillow imagehash tqdm
```

FFmpeg installation varies by platform. On Android (Termux), you can install it with:

```bash
pkg install ffmpeg
```

## Usage

### Basic Usage

```bash
python dupe.py -d /path/to/video/directory
```

### Options

```
-d, --directory PATH         The directory path to scan for videos (required)
-s, --hash-size INT          Hash size (power of 2) for dHash (default: 8)
-t, --threshold INT          Hamming distance threshold for considering a match.
                             Lower is more strict (default: 5)
--sub                        Include subdirectories in the scan
--threads INT               Number of threads to use for processing (default: 4)
--sec SECONDS               Comma-separated list of seconds to extract thumbnails
                            Example: 5,30 (default: 5)
--delete                    Delete duplicate videos after finding them
--delete-from-json          Delete duplicates based on duplicate_videos.json file
--min-match FLOAT           Minimum match percentage to consider a duplicate for 
                            deletion (default: 90.0)
```

### Examples

#### Scan a directory with default settings:
```bash
python dupe.py -d /path/to/my/videos
```

#### Scan with subdirectories and custom hash settings:
```bash
python dupe.py -d /path/to/my/videos --sub --hash-size 16 --threshold 10
```

#### Use multiple timestamps for comparison:
```bash
python dupe.py -d /path/to/my/videos --sec 5,30,60
```

#### Delete duplicates after finding them:
```bash
python dupe.py -d /path/to/my/videos --delete
```

#### Delete duplicates with a lower match threshold:
```bash
python dupe.py -d /path/to/my/videos --delete --min-match 80.0
```

#### Delete duplicates based on previously generated JSON file:
```bash
python dupe.py --delete-from-json --min-match 85.0
```

## How it Works

1. The script scans a given directory (and optionally its subdirectories) for video files.
2. For each video file, it extracts thumbnails at the specified time intervals using FFmpeg.
3. Perceptual hashes (dHash) are calculated for each extracted thumbnail.
4. The hashes from each video are compared to identify duplicates.
5. Videos with an average hash distance below the threshold are considered duplicates.
6. The results are saved to a JSON file named `duplicate_videos.json`.

## Output

The script creates two outputs:

1. A JSON file (`duplicate_videos.json`) containing the detected duplicates with their match percentages
2. Console output showing progress and results

## Match Percentage Calculation

The match percentage is calculated as:
```
match_percentage = (1 - (average_distance / total_hash_bits)) * 100
```

Where:
- `average_distance` is the average Hamming distance across all compared hash pairs
- `total_hash_bits` is the total number of bits in the hash (hash_size × hash_size)

## Safety Notice

- Always review the results before deleting files
- The script creates a JSON file with results before asking for deletion confirmation
- Use the `--min-match` option to control how strict the duplicate detection is

## License

This script is provided as-is without any warranty. Use at your own risk.