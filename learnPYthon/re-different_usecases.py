# re_regex_quick_demo.py — concise, function-free regex cheatsheet
# Run directly. Outputs are printed inline.
#
# This file demonstrates many common `re` usages. Below each operation there
# are explanatory comments that describe what the regex tokens mean and why
# the expression is used that way.
#
# Common token reminders used throughout:
#  - \d        : digit character (0-9)
#  - \D        : non-digit character (inverse of \d)
#  - \w        : word character (letters, digits, underscore)
#  - \W        : non-word character (inverse of \w)
#  - \s        : whitespace (space, tab, newline)
#  - \S        : non-whitespace
#  - .         : any character except newline (unless DOTALL / re.S)
#  - ^ / $     : start / end of string (or line with MULTILINE)
#  - \b        : word boundary (position between word and non-word character)
#  - [...]     : character class (set of allowed characters)
#  - [^...]    : negated character class (anything not in the set)
#  - {n}       : exactly n repeats of the previous atom
#  - {n,m}     : between n and m repeats
#  - ?         : zero or one (makes previous token optional)
#  - *         : zero or more (greedy)
#  - +         : one or more (greedy)
#  - *? / +?   : non-greedy versions of * and +
#  - (...)     : capturing group (can be referenced by \1, \2 or m.group(1))
#  - |         : alternation (logical OR)
#  - r"..."    : raw string literal in Python — avoids having to escape backslashes twice
#
# When reading the inline comments below, pay attention to:
#  - why a token is used (e.g. \b to ensure whole PAN is matched)
#  - how quantifiers like {5} mean EXACTLY five occurrences of the preceding token
#  - how replacements access matched groups via m.group() or backreferences like \3

import re

print("\n=== 0) Sample data ===")
name  = "Sonica testinoag @1234 2025"
name2 = "Sonica testinoag @1234 2025 TESTIOA"
names = [
    "VenuFromHYD@1234.com","Asha_Kumar#9876.net","RaviTeja$2025@mail.org",
    "Priya.Singh@4507.io","Nikhil@Tech_9090.in","Meena#Star$7788.biz",
    "Suresh.Kumar@Alpha123.co","Latha_Reddy$5678@gmail.com",
    "Kiran@BlueSky#9012.com","Anita.Dev@World$6543.net",
]
print("name =", name); print("name2 =", name2)

# 1) sub / subn — replace
print("\n=== 1) re.sub / re.subn — replace ===")
print("replace 'a' -> '*':", re.sub(r"a", "*", name))
# r"a" matches the literal character 'a'. re.sub replaces all occurrences by default.

print("replace first 'a' only:", re.sub(r"a", "*", name, count=1))
# 'count=1' limits replacements to the first match.

print("vowels -> '*':", re.sub(r"[aeiou]", "*", name))
# [aeiou] is a character class matching any one vowel (a OR e OR i OR o OR u).

print("letters -> '*':", re.sub(r"[a-zA-Z]", "*", name2))
# [a-zA-Z] matches any ASCII letter (lowercase a-z or uppercase A-Z).

print("letters+digits removed:", re.sub(r"[a-zA-Z0-9]", "", name2))
# [a-zA-Z0-9] matches letters or digits; replacing with "" removes them.

print("non-alnum removed (keep _ and space):", re.sub(r"[^a-zA-Z0-9_ ]", "", name2))
# [^...] is negation: this removes any character that is NOT a letter, digit, underscore, or space.

print("\nBad character-class range example and fix:")
try:
    # WRONG example: r"[^a-zA-Z0- ]" is ambiguous because '-' can define a range in a class.
    # The pattern here unintentionally creates a range '0- ' (which is invalid).
    print(re.sub(r"[^a-zA-Z0- ]", "", name2))  # WRONG: '-' forms a range '0- '
except re.error as e:
    print("Error:", e)
    # Fix: escape the hyphen with \- or place it at the start or end of the class.
    print("Fix (escape '-' or put at end):", re.sub(r"[^a-zA-Z0\- ]", "", name2))
    # r"[^a-zA-Z0\- ]" — here \- means a literal hyphen inside the class.

new_s, n = re.subn(r"red", "GREEN", "red red blue red")
print("\nsubn returns (string, count):", (new_s, n))
# re.subn returns a tuple (new_string, number_of_subs_made)

# 2) Bulk cleaning with sub
print("\n=== 2) Bulk cleaning (list comprehension with sub) ===")
print("alnum only:", [re.sub(r"[^a-zA-Z0-9]", "", x) for x in names])
# each name has non-alnum removed

print("letters only:", [re.sub(r"[^a-zA-Z]", "", x) for x in names])
# each name retains only letters

# 3) findall — all matches
print("\n=== 3) re.findall — all matches ===")
s = "Phones: +91-98765-43210, 040-12345678, ext 1234"

print("phones (simple):", re.findall(r"\+91-\d{5}-\d{5}|\d{3}-\d{8}", s))
# Explanation:
#  - \+        : literal plus sign (escaped because '+' is special in regex)
#  - 91-       : literal characters '91-'
#  - \d{5}     : exactly 5 digits (quantifier {5} means repeat previous token 5 times)
#  - -\d{5}    : hyphen then 5 digits
#  - |         : alternation (OR) — match either the +91-style number OR the other pattern
#  - \d{3}-\d{8} : pattern for e.g. '040-12345678' (3 digits, hyphen, 8 digits)
#
# So the expression finds either +91-xxxxx-xxxxx OR xxx-xxxxxxxx.

print("digits groups:", re.findall(r"(\d+)", s))
# (\d+) is a capturing group that matches one-or-more digits; findall returns the digits sequences.

# 4) finditer — matches with positions
print("\n=== 4) re.finditer — matches with positions ===")
for m in re.finditer(r"\d+", s):
    print("value:", m.group(), "span:", m.span())
# re.finditer yields Match objects; m.span() gives (start_index, end_index).

# 5) split — split by regex
print("\n=== 5) re.split — split by regex ===")
line = "name:venu|age:36|city:hyd"
print("split by '|' ->", re.split(r"\|", line))
# r"\|" splits on the literal pipe character (escaped because '|' is special in regex)

print("split by ':' or '|' ->", re.split(r"[:|]", line))
# Inside [] the '|' is literal, so this matches either ':' OR '|' — equivalent to r"[:|]"

print("keep delimiters (capture them) ->", re.split(r"([:|])", line))
# Capturing the delimiter with (...) makes split include the delimiters in the output list.

# 6) match vs search vs fullmatch
print("\n=== 6) match vs search vs fullmatch ===")
t = "Order#123 shipped"
print("match '^Order#\\d+' ->", bool(re.match(r"Order#\d+", t)))
# re.match checks from the beginning of the string (like '^' at the start).
# \d+ matches one or more digits after 'Order#'.

print("search '#\\d+' ->", bool(re.search(r"#\d+", t)))
# re.search finds the pattern anywhere in the string.

print("fullmatch '\\d+' on '123' ->", bool(re.fullmatch(r"\d+", "123")))
# re.fullmatch requires the entire string to match the pattern.

# 7) compile — precompile + flags
print("\n=== 7) re.compile — precompile + flags ===")
pat = re.compile(r"\b[a-z]{3}\b", flags=re.I)   # 3-letter words, ignore case
print("3-letter words:", pat.findall("The fox and DOG ran to Hyd."))
# Explanation:
#  - \b      : word boundary — ensures the 3-letter token is a whole word, not part of a larger word
#  - [a-z]{3}: exactly three letters
#  - flags=re.I sets case-insensitive matching

# Flags demo
text = "First\nsecond\nTHIRD"
print("MULTILINE ^ at each line:", re.findall(r"^.\w+", text, flags=re.M))
# re.M or MULTILINE makes ^ and $ match at each line boundary.

print("DOTALL '.' matches newline:", bool(re.search(r".+", "a\nb", flags=re.S)))
# re.S (DOTALL) makes '.' match newline as well.

# 8) escape — treat user input literally
print("\n=== 8) re.escape — treat user input literally ===")
needle = 'a+b? (x)'
print("escaped:", re.escape(needle))
# re.escape escapes regex metacharacters so the string can be used literally in a pattern.
print("literal search works:", bool(re.search(re.escape(needle), 'xx a+b? (x) yy')))

# 9) Greedy vs non-greedy
print("\n=== 9) Greedy vs non-greedy ===")
html = "<tag>one</tag><tag>two</tag>"
print("greedy:", re.findall(r"<tag>.*</tag>", html))
# '.*' is greedy and will match as much as possible, producing a single large match.
print("non-greedy:", re.findall(r"<tag>.*?</tag>", html))
# '.*?' is non-greedy (lazy) — matches the smallest possible content for each <tag>...</tag>.

# 10) Small practicals
print("\n=== 10) Small practicals ===")
print("format numbers:", re.sub(r"\b\d{4,}\b", lambda m: f"{int(m.group()):,}", "Totals: 1200 45000 9999999"))
# Explanation:
#  - \b           : word boundary ensures whole-number matching
#  - \d{4,}       : match sequences of 4 or more digits (quantifier with lower bound)
#  - the lambda formats the matched number using Python integer formatting (thousands separator)

phones = ["p:+919901788833", "040-12345678", "(+91) 99 99 88 88 33"]
print("digits only:", [re.sub(r"\D", "", p) for p in phones])
# r"\D" matches any non-digit; replacing with "" leaves digits only (common to normalize phone numbers)

text = "PAN ABCDE1234F and Aadhaar 1234 5678 9012"
# This line masks the PAN using a lambda replacement. Let's break it down:
# Pattern: r"\b[A-Z]{5}\d{4}[A-Z]\b"
#  - \b           : word boundary — ensures we match the whole PAN token, not a substring
#  - [A-Z]{5}     : exactly 5 uppercase letters (the PAN prefix)
#  - \d{4}        : exactly 4 digits
#  - [A-Z]        : exactly 1 uppercase letter (the PAN suffix)
#  - \b           : word boundary at the end
# So the full pattern matches PAN codes like 'ABCDE1234F'.
#
# Replacement: lambda m: m.group()[:2] + "XXXX" + m.group()[-3:]
#  - m.group() returns the entire matched PAN (e.g. 'ABCDE1234F')
#  - m.group()[:2] keeps the first two characters ('AB')
#  - "XXXX" is literal masking characters inserted to hide middle characters
#  - m.group()[-3:] keeps the last three characters ('34F') in this example
# NOTE: this replacement preserves some start/end chars and hides the middle with 'XXXX'.
#       The slice choices determine exactly which positions remain visible.
text = re.sub(r"\b[A-Z]{5}\d{4}[A-Z]\b", lambda m: m.group()[:2] + "XXXX" + m.group()[-3:], text)

# Aadhaar masking: pattern r"\b(\d{4})[ -]?(\d{4})[ -]?(\d{4})\b"
#  - (\d{4})      : first capturing group of 4 digits
#  - [ -]?        : optional space or hyphen separator between blocks (matches ' ' or '-')
#  - (\d{4})      : second capturing group of 4 digits
#  - [ -]?        : optional separator
#  - (\d{4})      : third capturing group of 4 digits
#  - \b           : word boundary to ensure whole token
# Replacement r"XXXX XXXX \3" uses the third capture group (the last 4 digits) to keep them visible.
text = re.sub(r"\b(\d{4})[ -]?(\d{4})[ -]?(\d{4})\b", r"XXXX XXXX \3", text)
print("masked:", text)

print("\nDone.")