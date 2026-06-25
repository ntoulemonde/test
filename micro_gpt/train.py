# from https://microgpt.jtauber.com/
# step 0
# %%
import os
import random
import math

random.seed(20260625)
echo = False
# %%
# Load dataset
if not os.path.exists("input.txt"):
    import urllib.request
    names_url = 'https://raw.githubusercontent.com/karpathy/makemore/refs/heads/master/names.txt'
    urllib.request.urlretrieve(names_url, 'input.txt')
# %%
docs = [l.strip() for l in open("input.txt").read().strip().split("\n") if l.strip()]
random.shuffle(docs)
print(f"num docs: {len(docs)}")
# %%
uchars = sorted(set(''.join(docs)))
BOS = len(uchars)
vocab_size = len(uchars) + 1

if echo: 
    print(uchars)
    print(f"vocab size: {vocab_size}")
# %%

state_dict = [[0] * vocab_size for _ in range(vocab_size)]

# %%
def bigram(token_id):
    row = state_dict[token_id]
    total = sum(row) + vocab_size
    return [(next_token_i+1)/total for next_token_i in row]


if echo:
    print(bigram(0))
    print(bigram(26))
    print(sum(bigram(26)))

# %%
num_steps = 100000
# echo = False
for step in range(num_steps):
    word = docs[step % len(docs)] 
    tokens = [BOS] + [uchars.index(letter) for letter in word] + [BOS]
    n = len(tokens) - 1
    if echo: 
        print(f"{word} -> {n} tokens : {tokens}")

    # Calculate loss
    losses = []
    for pos_id in range(n):
        token_id, target_id = tokens[pos_id], tokens[pos_id + 1]
        probs = bigram(token_id)
        loss_t = -math.log(probs[target_id])
        losses.append(loss_t)
    loss = (1 / n) * sum(losses)
    if echo: 
        print(f"loss is {loss:.2f}")

    # Update count
    for pos_id in range(n):
        token_id, target_id = tokens[pos_id], tokens[pos_id + 1]
        state_dict[token_id][target_id] += 1
# %%

def print_state_dict(proba=False):
    if proba: 
        state_dict_local = [[f"{prob:0.2f}".replace("0.", ".").replace(".00", "") for prob in bigram(_)] for _ in range(vocab_size)]
    else: 
        state_dict_local = state_dict
    legend = [[char.upper() for char in uchars] + ["BOS"]]
    state_dict_legend =  [[legend[0][i]] + state_dict_local[i] for i in range(vocab_size)]
    state_dict_legend = [[""] + [char.upper() for char in uchars] + ["BOS"]] + state_dict_legend
    
    max_widths = [max(len(str(row[i])) for row in state_dict_legend) for i in range(len(state_dict_legend[0]))]

    # Print each row with padding
    for row in state_dict_legend:
        print(" ".join(f"{num:>{width}}" for num, width in zip(row, max_widths)))

echo = True
if echo: 
    print_state_dict(proba=True)
# %%
for sample_idx in range(20):
    token_id = BOS
    sample = []
    for _ in range(16):
        token_id = random.choices(range(vocab_size), weights=bigram(token_id))[0]
        if token_id == BOS:
            break
        sample.append(uchars[token_id])
    print(f"sample {sample_idx+1:2d}: {''.join(sample)}")

# %%
