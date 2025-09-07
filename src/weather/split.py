import os

NUM_SHARDS = 4
input_file = "reqlinks.txt"
output_dir = "shards"

# Create output directory
os.makedirs(output_dir, exist_ok=True)

# Read all lines
with open(input_file, "r") as f:
    lines = [line.strip() for line in f if line.strip()]

# Split into evenly-sized chunks
shard_size = len(lines) // NUM_SHARDS
for i in range(NUM_SHARDS):
    start = i * shard_size
    end = (i + 1) * shard_size if i != NUM_SHARDS - 1 else len(lines)
    with open(f"{output_dir}/urls_{i}.txt", "w") as f:
        f.write("\n".join(lines[start:end]) + "\n")

print(f"✅ Split {len(lines)} lines into {NUM_SHARDS} files in '{output_dir}'")
