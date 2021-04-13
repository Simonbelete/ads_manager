import concurrent.futures
from time import time, sleep

start = time.perf_counter()

while True:
    print(time())

def do_something(seconds):
    print(f'Slepping {seconds} second...')
    time.sleep(seconds)
    return f'Done Slepping...{seconds}'

with concurrent.futures.ProcessPoolExecutor() as executor:
    secs = [5, 4, 3, 2, 1]
    results = [executor.submit(do_something, sec) for sec in secs]

    for f in concurrent.futures.as_completed(results):
        print(f.result())

# proceses = []
# for _ in range(10):
#     p = multiprocessing.Process(target=do_something, args=[1.5])
#     p.start()
#     proceses.append(p)
# for process in proceses:
#     process.join()

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)}')


# Process 1 -> Listen for changes
#     campaign.append(
#         {
#             start_second: current_second,
#             run_for: 500
#         }
#     )

# current_second = 0
# while True:
#     campigns = [ 
#         {
#             start_second: 0,
#             run_for: 900
#         }
#     ]

#     if current_second == (start_second + run_for):
#         stop campaing for campigns[current_second]
#         remove campigns[current_second]