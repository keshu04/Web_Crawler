# ranker.py

import csv
import random
from collections import defaultdict

# --- CONSTANTS AND CONFIGURATION ---

# PageRank parameters
DAMPING = 0.85
SAMPLES = 10000

# Color codes for console output
RED = "\033[31m"
C_END = "\033[0m"

# --- RANKER FUNCTIONS ---

def read_corpus():
    """Reads the domain graph from the pagerank.csv file."""
    pages = defaultdict(set)
    try:
        with open("OUTPUT/pagerank.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if row:
                    pages[row[0]].update(x for x in row[1:] if x)
    except FileNotFoundError:
        print(f"{RED}pagerank.csv not found. Cannot run ranker.{C_END}")
        return None
    
    # Ensure all linked pages are also keys in the corpus
    all_pages = set(pages.keys()).union(*pages.values())
    for page in all_pages:
        if page not in pages:
            pages[page] = set()
    return dict(pages)

def counter_ranker(corpus):
    """Ranks pages based on the number of outgoing links."""
    return {page: len(links) for page, links in corpus.items()}

def transition_model(corpus, page, damping_factor):
    """
    Returns a probability distribution over which page to visit next.
    With probability `damping_factor`, a random link from the current page is chosen.
    With probability `1 - damping_factor`, a random page from the entire corpus is chosen.
    """
    prob_dist = {}
    linked_pages = corpus.get(page, set())
    num_links = len(linked_pages)
    num_total_pages = len(corpus)
    
    if num_links > 0:
        prob_from_link = damping_factor / num_links
        prob_from_all = (1 - damping_factor) / num_total_pages
        for p in corpus:
            prob_dist[p] = prob_from_all + (prob_from_link if p in linked_pages else 0)
    else:
        # If a page has no links, assume it links to all pages with equal probability
        for p in corpus:
            prob_dist[p] = 1 / num_total_pages
    return prob_dist

def sample_pagerank(corpus, damping_factor, n):
    """Estimates PageRank by taking `n` random samples from the transition model."""
    pagerank = {page: 0 for page in corpus}
    if not corpus:
        return pagerank
        
    current_page = random.choice(list(corpus.keys()))
    for _ in range(n):
        pagerank[current_page] += 1
        model = transition_model(corpus, current_page, damping_factor)
        pages, probabilities = zip(*model.items())
        current_page = random.choices(pages, weights=probabilities, k=1)[0]
        
    return {page: count / n for page, count in pagerank.items()}

def iterate_pagerank(corpus, damping_factor):
    """Calculates PageRank by repeatedly updating values until they converge."""
    num_pages = len(corpus)
    if num_pages == 0:
        return {}
        
    pagerank = {page: 1 / num_pages for page in corpus}
    incoming_links = {p: {linker for linker, links in corpus.items() if p in links} for p in corpus}
    
    while True:
        new_pagerank = {}
        for page in corpus:
            random_surf_prob = (1 - damping_factor) / num_pages
            link_prob = sum(pagerank[linker] / len(corpus[linker]) for linker in incoming_links[page] if corpus[linker])
            new_pagerank[page] = random_surf_prob + damping_factor * link_prob
        
        max_change = max(abs(pagerank[p] - new_pagerank[p]) for p in corpus)
        pagerank = new_pagerank
        if max_change < 0.001:
            break
            
    # Normalize to ensure the sum is 1
    total = sum(pagerank.values())
    return {p: rank / total for p, rank in pagerank.items()}

def print_ranks(ranks, title):
    """Prints a formatted table of ranked domains."""
    print("\n" + "-" * (len(title) + 4))
    print(f"  {title}")
    print("-" * (len(title) + 4))
    print("\n" + "." * 60)
    print(f"  {'Rank':<5} {'Domain Name':<40} {'Score'}")
    print("." * 60)

    sorted_ranks = sorted(ranks.items(), key=lambda item: item[1], reverse=True)
    for i, (page, rank) in enumerate(sorted_ranks, 1):
        print(f"  {i:<5} {page:<40} {rank:.6f}")
    print("-" * 60)