# main.py

import time
import csv
import sys

from crawler import Crawler, RED, C_END
from ranker import (read_corpus, counter_ranker, sample_pagerank, 
                    iterate_pagerank, print_ranks, DAMPING, SAMPLES)

# --- MAIN EXECUTION BLOCK ---

def main():
    # --- Part 0: Get User Input Interactively ---
    print("--- Configure the Web Crawler ---")
    try:
        max_links = int(input("Enter Max Links per Page: "))
        pages_limit = int(input("Enter Page Download Limit: "))
        max_threads = int(input("Enter Max Concurrent Threads: "))

        # Get ranker choice from user
        ranker_flag = ''
        ranker_options = {'1': '-n', '2': '-sp', '3': '-ip'}
        while ranker_flag not in ranker_options.values():
            print("\nChoose a ranking algorithm:")
            print("  1: Counter (Fastest, simple link count)")
            print("  2: Sampling PageRank")
            print("  3: Iterative PageRank (Most accurate)")
            choice = input("Enter your choice (1, 2, or 3): ")
            if choice in ranker_options:
                ranker_flag = ranker_options[choice]
            else:
                print(f"{RED}Invalid choice. Please enter 1, 2, or 3.{C_END}")

    except ValueError:
        print(f"\n{RED}Invalid input. Please enter whole numbers for limits and threads.{C_END}")
        sys.exit(1) # Exit the script if input is bad

    # === Part 1: Crawling ===
    print("\n--- Starting Crawler ---")
    start_time = time.perf_counter()
    my_crawler = Crawler(max_links, pages_limit, max_threads)
    my_crawler.initialize()
    my_crawler.run_crawler()
    my_crawler.show_results()
    my_crawler.close()
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    
    print("\nCRAWLING FINISHED.")
    print(f"{RED}Total elapsed time: {elapsed_ms:.2f} ms{C_END}")

    with open("OUTPUT/crawler_timings.csv", "a", newline="") as f:
        csv.writer(f).writerow([my_crawler.max_threads, elapsed_ms])

    # === Part 2: Ranking ===
    print("\n--- Calculating Ranks ---")
    corpus = read_corpus()
    if not corpus:
        print(f"{RED}Could not read corpus data from OUTPUT/pagerank.csv to perform ranking.{C_END}")
        return
        
    if ranker_flag == "-n":
        ranks = counter_ranker(corpus)
        print_ranks(ranks, "Domain Name Rankings (by Outgoing Link Count)")
    elif ranker_flag == "-sp":
        ranks = sample_pagerank(corpus, DAMPING, SAMPLES)
        print_ranks(ranks, f"Domain Name Rankings (PageRank via Sampling, n={SAMPLES})")
    elif ranker_flag == "-ip":
        ranks = iterate_pagerank(corpus, DAMPING)
        print_ranks(ranks, "Domain Name Rankings (PageRank via Iteration)")

if __name__ == "__main__":
    main()