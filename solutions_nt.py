# Cf https://inseefrlab.github.io/formation-python-initiation/source/projects/puissance4/tutorial.html

import copy 
import solutions

def initialize_grid(n_rows=6, n_columns=7):
    empty_list = [[' '] * n_columns for _ in range(n_rows)]
    return empty_list

grid_solution = initialize_grid()
print(f'Nombre de lignes : {len(grid_solution)}')
print(f'Nombre de colonnes : {len(grid_solution[0])}')

def display_grid(grid_p4):
    for row in grid_p4:
        print("| " +  " | ".join(row) + " |")

col = 1
color = 'R'

def make_move(grid, column_to_play, disc_color):
    if disc_color not in ['R', 'J']:
        raise ValueError("Error: Argument must be 'R' or 'J'")

    elif column_to_play - 1  not in range(len(grid)):
        raise ValueError(f'Error: col number must be between 1 and {len(grid[0])}')

    else:
        row = len(grid) - 1
        while grid[row][column_to_play-1] != ' ':
            row -= 1

        grid[row][column_to_play-1] = disc_color

        return grid


grid = initialize_grid()  # Initialisation
grid = make_move(grid=grid, column_to_play=2, disc_color="R")  # 1er tour de jeu
grid = make_move(grid=grid, column_to_play=5, disc_color="J")  # 2ème tour de jeu
grid = make_move(grid=grid, column_to_play=2, disc_color="R")  # 3ème tour de jeu
grid = make_move(grid=grid, column_to_play=2, disc_color="Z")  # Erreur
grid = make_move(grid=grid, column_to_play=1, disc_color="R")  # Pr victoire au deuxième rang en partant bas
grid = make_move(grid=grid, column_to_play=3, disc_color="J")
grid = make_move(grid=grid, column_to_play=4, disc_color="R")
grid = make_move(grid=grid, column_to_play=1, disc_color="R")  # Pr victoire
grid = make_move(grid=grid, column_to_play=3, disc_color="R")
grid = make_move(grid=grid, column_to_play=4, disc_color="R")
display_grid(grid)

import re

# Détection d'une victoire (horizontale) sur une ligne
def check_row_victory(row):
    pattern = r'RRRR|JJJJ'
    res = len(re.findall(pattern, ''.join(row)))
    return res > 0


def check_horizontal_victory(grid):
    res = False
    for grid_row in grid:
        if check_row_victory(grid_row):
            res = True
            break

    return res


print(check_row_victory(grid[3]))  # Renvoie False
print(check_row_victory(grid[4]))  # Renvoie True
print(check_row_victory(grid[5]))  # Renvoie False

check_horizontal_victory(grid=grid)
display_grid(grid)


def check_vertical_victory(grid):
    res = False

    for nb_col in range(len(grid[0])):
        col=[]
        for row in grid:
            col.append(row[nb_col])
            if check_row_victory(col):
                res = True
                break

    return res


# Détection d'une victoire (verticale) sur une grille
grid_solution = initialize_grid()  # Initialisation
print(check_vertical_victory(grid_solution))  # Renvoie False
print()  # Retour à la ligne

grid_solution = make_move(grid=grid_solution, column_to_play=2, disc_color="J")
grid_solution = make_move(grid=grid_solution, column_to_play=2, disc_color="J")
grid_solution = make_move(grid=grid_solution, column_to_play=2, disc_color="J")
grid_solution = make_move(grid=grid_solution, column_to_play=2, disc_color="J")
display_grid(grid_solution)
print()  # Retour à la ligne

print(check_vertical_victory(grid_solution))  # Renvoie True

def check_victory(grid):
    return check_vertical_victory(grid) | check_vertical_victory(grid)

def make_move_and_check_victory(grid, column_to_play, disc_color):
    grid = make_move(grid, column_to_play, disc_color)
    display_grid(grid)
    if check_victory(grid):
        print("FIN DE PARTIE")
    return grid


# Vérification du résultat
grid = initialize_grid()  # Initialisation
print("Tour 1")
grid = make_move_and_check_victory(grid=grid, column_to_play=2, disc_color="J")
print("Tour 2")
grid = make_move_and_check_victory(grid=grid, column_to_play=2, disc_color="J")
print("Tour 3")
grid = make_move_and_check_victory(grid=grid, column_to_play=2, disc_color="J")
print("Tour 4")
grid = make_move_and_check_victory(grid=grid, column_to_play=2, disc_color="J")
