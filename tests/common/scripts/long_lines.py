import sys

line_a = "a" * 1024 * 1024
line_b = "b" * 1024 * 1024

print(line_a)
print(line_b, file=sys.stderr)
print(line_a, flush=True)
print(line_b, file=sys.stderr, flush=True)

# without new lines
print(line_b, file=sys.stderr, end="")
print(line_a, end="")
