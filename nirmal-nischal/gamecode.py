"""
Simple console Sudoku game
- Play interactively in the terminal by running this script.
- Commands:
    set r c v   -> set value v (1-9) at row r and column c (1-based)
    clear r c   -> clear the cell at row r, column c (set to 0)
    row r v1..v9 -> update entire row r with nine tokens; use '.' or '0' to keep a cell unchanged
    hint        -> fill one empty cell using the solver
    solve       -> show full solution
    validate    -> check current board validity (no conflicts)
    show        -> print the board
    restart     -> reset to original puzzle
    quit / exit -> exit the game
- For quick verification run: python gamecode.py --test
  which will solve the sample puzzle and print the solved board then exit.
"""

from copy import deepcopy
import sys
import re

# Sample Sudoku puzzle (0 denotes empty cells)
SAMPLE_PUZZLE = [
    [5, 3, 0, 0, 7, 0, 0, 0, 0],
    [6, 0, 0, 1, 9, 5, 0, 0, 0],
    [0, 9, 8, 0, 0, 0, 0, 6, 0],
    [8, 0, 0, 0, 6, 0, 0, 0, 3],
    [4, 0, 0, 8, 0, 3, 0, 0, 1],
    [7, 0, 0, 0, 2, 0, 0, 0, 6],
    [0, 6, 0, 0, 0, 0, 2, 8, 0],
    [0, 0, 0, 4, 1, 9, 0, 0, 5],
    [0, 0, 0, 0, 8, 0, 0, 7, 9],
]

def print_board(board):
    """Prints the board to the console in a readable 9x9 format."""
    sep = "+-------+-------+-------+"
    for i, row in enumerate(board):
        if i % 3 == 0:
            print(sep)
        row_str = "|"
        for j, val in enumerate(row):
            ch = str(val) if val != 0 else '.'
            row_str += ' ' + ch + ' '
            if (j + 1) % 3 == 0:
                row_str += '|'
        print(row_str)
    print(sep)


def find_empty(board):
    """Return (r, c) of first empty cell or None if full."""
    for r in range(9):
        for c in range(9):
            if board[r][c] == 0:
                return r, c
    return None


def is_valid(board, r, c, num):
    """Check if placing num at (r, c) is valid per Sudoku rules."""
    # row
    if any(board[r][j] == num for j in range(9)):
        return False
    # column
    if any(board[i][c] == num for i in range(9)):
        return False
    # 3x3 box
    br, bc = 3 * (r // 3), 3 * (c // 3)
    for i in range(br, br + 3):
        for j in range(bc, bc + 3):
            if board[i][j] == num:
                return False
    return True


def solve(board):
    """Backtracking solver. Modifies board in-place. Returns True if solved."""
    empty = find_empty(board)
    if not empty:
        return True
    r, c = empty
    for num in range(1, 10):
        if is_valid(board, r, c, num):
            board[r][c] = num
            if solve(board):
                return True
            board[r][c] = 0
    return False


def board_is_valid(board):
    """Check current board for conflicts (doesn't require completeness)."""
    for r in range(9):
        for c in range(9):
            val = board[r][c]
            if val == 0:
                continue
            # Temporarily clear and test
            board[r][c] = 0
            if not is_valid(board, r, c, val):
                board[r][c] = val
                return False, (r, c, val)
            board[r][c] = val
    return True, None


def interactive_game(puzzle):
    original = deepcopy(puzzle)
    board = deepcopy(puzzle)
    solved_board = deepcopy(puzzle)
    if not solve(solved_board):
        print("Error: sample puzzle has no solution.")
        return

    print("Welcome to Console Sudoku! Enter commands (type 'help' for instructions).")
    print_board(board)

    while True:
        try:
            cmd = input('> ').strip()
        except (EOFError, KeyboardInterrupt):
            print('\nExiting Sudoku. Goodbye!')
            break
        if not cmd:
            continue
        parts = cmd.split()
        action = parts[0].lower()

        if action in ('quit', 'exit'):
            print('Goodbye!')
            break
        elif action == 'help':
            print("Commands: set r c v [--force] | clear r c | row r v1..v9 [--force] | hint | solve | validate | show | restart | quit")
        elif action == 'show':
            print_board(board)
        elif action == 'restart':
            board = deepcopy(original)
            print('Board reset to original puzzle.')
            print_board(board)
        elif action == 'validate':
            ok, info = board_is_valid(board)
            if ok:
                print('Board has no conflicts (so far).')
            else:
                r, c, val = info
                print(f'Conflict detected at row {r+1}, col {c+1}, value {val}')
        elif action == 'solve':
            print('Solution:')
            print_board(solved_board)
            # Optionally ask if user wants to accept solution
        elif action == 'hint':
            # find one empty and fill with solved value
            empty = find_empty(board)
            if not empty:
                print('Board is already complete.')
            else:
                r, c = empty
                board[r][c] = solved_board[r][c]
                print(f'Hint placed at row {r+1}, col {c+1}.')
                print_board(board)
        elif action == 'clear':
            if len(parts) != 3:
                print('Usage: clear r c')
                continue
            try:
                r = int(parts[1]) - 1
                c = int(parts[2]) - 1
            except ValueError:
                print('Row/col must be integers')
                continue
            if not (0 <= r < 9 and 0 <= c < 9):
                print('Row/col out of range (1-9)')
                continue
            if original[r][c] != 0:
                print('Cannot clear original given cells.')
                continue
            board[r][c] = 0
            print_board(board)
        elif action == 'set':
            # Accept optional --force flag anywhere in the command
            if len(parts) < 4:
                print('Usage: set r c v [--force]')
                continue
            try:
                r = int(parts[1]) - 1
                c = int(parts[2]) - 1
                v = int(parts[3])
            except ValueError:
                print('Row/col/value must be integers')
                continue
            if not (0 <= r < 9 and 0 <= c < 9 and 1 <= v <= 9):
                print('Row/col/value out of range (rows/cols:1-9, value:1-9)')
                continue
            # detect force flag in the remaining tokens
            tail = parts[4:]
            force = any(tok in ('--force', 'force') for tok in tail) or any(tok in ('--force','force') for tok in parts[0:4])
            if original[r][c] != 0 and not force:
                print('Cannot change original given cells. Use --force to override (may make puzzle invalid).')
                continue
            if not is_valid(board, r, c, v):
                print('Move invalid: violates Sudoku rules.')
                continue
            board[r][c] = v
            print_board(board)
            # check for completion
            if not find_empty(board):
                print('Board complete! Verifying...')
                ok, _ = board_is_valid(board)
                if ok:
                    print('Congratulations! You solved the Sudoku!')
                else:
                    print('Board complete but invalid.')
                break
        elif action == 'row':
            # Usage: row r v1 v2 v3 v4 v5 v6 v7 v8 v9
            # Use '.' or '0' to keep a cell unchanged. Only non-original cells may be modified.
            if len(parts) < 3:
                print('Usage: row r v1 v2 v3 v4 v5 v6 v7 v8 v9 (use "." for unchanged)')
                continue
            try:
                r = int(parts[1]) - 1
            except ValueError:
                print('Row must be an integer (1-9)')
                continue
            if not (0 <= r < 9):
                print('Row out of range (1-9)')
                continue
            # Check for force flag anywhere in the arguments
            force = False
            args_after = parts[2:]
            # if user included --force or force, set flag and remove it from args
            args_after = [a for a in args_after if a not in ('--force', 'force')]
            if len(parts) != len(args_after) + 2:
                force = True
            # Normalize input: allow pasted row strings containing '|' separators or commas
            # Join remaining parts, then split on pipes, spaces, or commas
            row_str = ' '.join(args_after)
            # Split on pipes, commas, or whitespace; ignore empty tokens
            tokens = [t for t in re.split(r'[\|,\s]+', row_str.strip()) if t != '']
            if len(tokens) != 9:
                print('Row must contain 9 tokens (you can paste the printed row, use "." for unchanged)')
                continue
            # Apply changes on a temporary board and validate
            temp = deepcopy(board)
            ok_change = True
            for j, tok in enumerate(tokens):
                tok = tok.strip()
                if tok in ('.', '0', '_'):
                    continue
                try:
                    v = int(tok)
                except ValueError:
                    print(f'Invalid token at position {j+1}: {tok} (must be 1-9 or ".")')
                    ok_change = False
                    break
                if not (1 <= v <= 9):
                    print(f'Value out of range at position {j+1}: {v}')
                    ok_change = False
                    break
                if original[r][j] != 0 and not force:
                    print(f'Cannot change original given cell at row {r+1}, col {j+1} (use --force to override)')
                    ok_change = False
                    break
                # Tentatively place and validate against current temp board
                temp[r][j] = v
                if not is_valid(temp, r, j, v):
                    print(f'Invalid placement at row {r+1}, col {j+1}: {v} (conflicts with existing cells)')
                    ok_change = False
                    break
            if ok_change:
                board = temp
                print(f'Row {r+1} updated.')
                print_board(board)
        else:
            print('Unknown command. Type help for instructions.')


def run_test_mode(puzzle):
    """Non-interactive self-test: solve puzzle and print solved board."""
    b = deepcopy(puzzle)
    ok = solve(b)
    if ok:
        print('Test run: solved board follows:')
        print_board(b)
    else:
        print('Test run: could not solve the puzzle (unsolvable).')


if __name__ == '__main__':
    # If called with --test, run non-interactive solver test and exit
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        run_test_mode(SAMPLE_PUZZLE)
        sys.exit(0)
    interactive_game(SAMPLE_PUZZLE)
