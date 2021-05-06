import json
from copy import deepcopy
from multiprocessing import Process, BoundedSemaphore, Queue
from os import cpu_count
from random import randint
from time import sleep, time
from typing import Any, NoReturn
from typing import Dict, Text


def worker(book: dict, author: Text, name: Text, q: Any, semaphore: Any = None,
           slp: float = 0) -> NoReturn:
    if author == book["Author"]:
        q.put([True, name, author])
    else:
        q.put([False])
    sleep(slp)
    if semaphore is not None:
        semaphore.release()


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

    KeywriteAuthor: Text = input("Enter the author of the book: ").rstrip(" ").lstrip("")

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
    PauseProcess: float = round(dataset["PT"] / 1000, 3)

    qauthors = Queue(MaxItemInStructure)
    sem = BoundedSemaphore(ParallelThread)

    print("=" * 10)
    pList = []
    ST_PARALLEL: float = round(time(), 1)
    for i in dataset["Books"]:
        BookInfo = dataset["Books"][i]
        BookName = i
        BookAuthor = dataset["Books"][i]["Author"]

        sem.acquire()
        p = Process(target=worker,
                    args=(BookInfo, KeywriteAuthor, BookName, qauthors, sem, PauseProcess))
        p.start()
        pList.append(p)

    for j in pList:
        j.join()
        pList.remove(j)

    ET_PARALLEL: float = round(time() - ST_PARALLEL, 1)
    Result["TP"] = ET_PARALLEL

    pResult = dict()
    while qauthors.qsize() > 0:
        resp = qauthors.get()
        if resp[0] is True:
            if pResult.get(resp[2]) is None:
                pResult[resp[2]] = [resp[1]]
            else:
                pResult[resp[2]].append(resp[1])

    if Result.get("ResultParallel") is None:
        Result["ResultParallel"] = deepcopy(pResult)
        pResult.clear()

    print(f"Time of execution parallel - {ET_PARALLEL} (s)")

    ST: float = round(time(), 1)
    for k in dataset["Books"]:
        sleep(PauseProcess)
        BookInfo = dataset["Books"][k]
        BookName = k
        BookAuthor = dataset["Books"][k]["Author"]
        sem.acquire()
        worker(BookInfo, KeywriteAuthor, BookName, qauthors, sem, PauseProcess)
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

    with open("output/output_semaphore.json", "w", encoding="utf8") as wr:
        json.dump(Result, wr, indent=1, ensure_ascii=False)
        print("Result -> ", Result)
        if dataset["N"] != NonGeneratedBooks:
            with open("output/output_semaphore_generatedBooks.json", "w",
                      encoding="utf8") as genstream:
                json.dump(dataset["Books"], genstream, indent=1)
