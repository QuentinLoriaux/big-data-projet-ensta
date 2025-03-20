import re
import sys

def process_file(filename):
    pattern = re.compile(r'Operation took (\d+\.\d+) seconds')

    with open(filename, 'r') as file:
        content = file.read()

    matches = pattern.findall(content)

    matches = [float(match) for match in matches]

    # tests 20 par 20 pour sans cache, 100 par 100 pour avec cache
    arrays = [matches[i:i + 100] for i in range(0, len(matches), 100) if i % 5 != 1]

    for array in arrays:
        average = sum(array) / len(array)
        print(f"Array: {array}\nAverage: {average}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python moyennes.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    process_file(filename)