import json
from copy import deepcopy
from multiprocessing import Pool, Queue
from os import cpu_count
from random import randint
from time import sleep, time
from typing import Dict, List, Text


def worker(book: dict, author: Text, name: Text, slp: float = 0) -> List:
    if author == book["Author"]:
        loc_list = [True, name, author]
    else:
        loc_list = [False]
    sleep(slp)
    return loc_list


def generateBook(books: Dict) -> Dict:
    baseBookName = "Book"
    baseBookIdx = 1
    if books.get(baseBookName + str(baseBookIdx)) is None:
        books[f"{baseBookName}{baseBookIdx}"] = {
            "Author": f"Author{randint(1, 99)}",
            "Publisher": f"Publisher{randint(1, 99)}",
            "Year": randint(2000, 2022),
            "Total pages": randint(10, 250)}
    else:
        randBookIdx = randint(2, 1000)
        if books.get(f"{baseBookName}{randBookIdx}") is None:
            books[f"{baseBookName}{randBookIdx}"] = {
                "Author": f"Author{randint(1, 99)}",
                "Publisher": f"Publisher{randint(1, 99)}",
                "Year": randint(2000, 2022),
                "Total pages": randint(10, 250)}
        elif books.get(f"{baseBookName}{randBookIdx}") is not None:
            books[f"{baseBookName}{randint(2, 1000)}"] = {
                "Author": f"Author{randint(1, 99)}",
                "Publisher": f"Publisher{randint(1, 99)}",
                "Year": randint(2000, 2022),
                "Total pages": randint(10, 250)}
    return books


if __name__ == '__main__':
    with open("input.json", "r", encoding="utf8") as rw:
        dataset: Dict = json.load(rw)
        NonGeneratedBooks: int = len(dataset["Books"])

    keywriteAuthor: Text = input("Enter the author of the book: ").rstrip(" ").lstrip("")

    ParallelThread: int = dataset["M"]
    MaxItemInStructure: int = dataset["N"]
    Result: Dict = dict()

    if dataset["N"] != len(dataset["Books"]):
        generateNumber = dataset["N"] - len(dataset["Books"])
        for i in range(0, generateNumber):
            dataset["Books"] = generateBook(dataset["Books"])

    if ParallelThread == 0:
        ParallelThread = cpu_count()

    # Pause ms -> seconds
    PauseProcess: float = round(dataset["PT"] / 1000, 2)

    qauthors = Queue(MaxItemInStructure)
    pool = Pool(ParallelThread)

    print("=" * 10)

    ST_PARALLEL: float = round(time(), 1)
    qauthors.put(pool.starmap(worker,
                              [(dataset["Books"][i], keywriteAuthor, i, PauseProcess) for i in
                               dataset["Books"]]))
    pool.close()
    pool.join()

    ET_PARALLEL: float = round(time() - ST_PARALLEL, 1)
    Result["TP"] = ET_PARALLEL
    print(f"Time of execution parallel - {ET_PARALLEL} (s)")

    pResult = dict()
    while qauthors.qsize() > 0:
        resp = qauthors.get()
        for item in resp:
            if item[0] is True:
                if pResult.get(item[2]) is None:
                    pResult[item[2]] = [item[1]]
                else:
                    pResult[item[2]].append(item[1])

    if Result.get("ResultParallel") is None:
        Result["ResultParallel"] = deepcopy(pResult)
        pResult.clear()

    foundBooks: List = []
    ST: float = round(time(), 1)
    for k in dataset["Books"]:
        sleep(PauseProcess)
        book = dataset["Books"][k]
        book_name = k
        book_author = dataset["Books"][k]["Author"]
        qauthors.put(worker(book, keywriteAuthor, book_name, slp=PauseProcess))
    ET: float = round(time() - ST, 1)

    Result["T1"] = ET
    print(f"Time of execution successively - {ET} (s)")
    print("=" * 10)

    while qauthors.qsize() > 0:
        resp = qauthors.get()
        if resp[0] is True:
            if pResult.get(resp[2]) is None:
                pResult[resp[2]] = [resp[1]]
            else:
                pResult[resp[2]].append(resp[1])

    if Result.get("ResultSignle") is None:
        Result["ResultSignle"] = deepcopy(pResult)
        pResult.clear()

    with open("output/output_pool.json", "w", encoding="utf8") as wr:
        json.dump(Result, wr, indent=1, ensure_ascii=False)
        print("Result -> ", Result)
        if dataset["N"] != NonGeneratedBooks:
            with open("output/output_pool_generatedBooks.json", "w", encoding="utf8") as genstream:
                json.dump(dataset["Books"], genstream, indent=1)
