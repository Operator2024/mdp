import json
import multiprocessing
import os
from random import randint
from time import sleep, time
from typing import Any, NoReturn
from typing import Dict, Text


def worker(book: dict, author: Text, name: Text, q: Any, semaphore: Any = None, slp: float = 0) -> NoReturn:
    # "Punishment Without Revenge": {"Author": "Lope de Vega", "Publisher": "Oberon books", "Year": 1631, "Total pages": 96}
    if author in book["Author"]:
        q.put([True, name, author])
    else:
        q.put([False])
    # print(sem)
    sleep(slp)
    if semaphore is not None:
        semaphore.release()


def generateBook(books: Dict) -> Dict:
    baseBookName = "Book"
    baseBookIdx = 1
    if books.get(baseBookName + str(baseBookIdx)) is None:
        #     "Nineteen Eighty-Four": {"Author": "George Orwell", "Publisher":  "Secker & Warburg", "Year": 1949, "Total pages": 328}
        books[f"{baseBookName}{baseBookIdx}"] = {"Author": f"Author{randint(1, 99)}", "Publisher": f"Publisher{randint(1, 99)}", "Year": randint(2000, 2022),
                                                 "Total pages": randint(10, 250)}
    else:
        books[f"{baseBookName}{randint(2, 1000)}"] = {"Author": f"Author{randint(1, 99)}", "Publisher": f"Publisher{randint(1, 99)}", "Year": randint(2000, 2022),
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
        ParallelThread = os.cpu_count()

    # Pause ms -> seconds
    PauseProcess: float = round(dataset["PT"] / 1000, 2)

    qauthors: object = multiprocessing.Queue(MaxItemInStructure)
    sem = multiprocessing.BoundedSemaphore(ParallelThread)

    pList = []
    ST_PARALLEL: float = round(time(), 1)
    for i in dataset["Books"]:
        # sleep(PauseProcess)
        BookInfo = dataset["Books"][i]
        BookName = i
        BookAuthor = dataset["Books"][i]["Author"]

        sem.acquire()
        p = multiprocessing.Process(target=worker, args=(BookInfo, KeywriteAuthor, BookName, qauthors, sem, PauseProcess))
        p.start()
        pList.append(p)
        # sleep(PauseProcess)

    for j in pList:
        j.join()
        pList.remove(j)

    ET_PARALLEL: float = round(time() - ST_PARALLEL, 1)
    Result["TP"] = ET_PARALLEL

    print(f"Time of execution parallel - {ET_PARALLEL} (s)")

    while qauthors.qsize() > 0:
        resp = qauthors.get()
        if resp[0] is True:
            if Result.get(resp[2]) is None:
                Result[resp[2]] = [resp[1]]
            else:
                Result[resp[2]].append(resp[1])

    if len(Result) == 1:
        Result[0] = "Author not found in the input dataset!"

    ST: float = round(time(), 1)
    for k in dataset["Books"]:
        sleep(PauseProcess)
        BookInfo = dataset["Books"][k]
        BookName = k
        BookAuthor = dataset["Books"][k]["Author"]
        worker(BookInfo, KeywriteAuthor, BookName, qauthors, slp=PauseProcess)
    ET: float = round(time() - ST, 1)
    Result["T1"] = ET
    print(f"Time of execution successively - {ET} (s)")

    with open("output/output_semaphore.json", "w", encoding="utf8") as wr:
        json.dump(Result, wr, indent=1, ensure_ascii=False)
        print(Result)
        if dataset["N"] != NonGeneratedBooks:
            with open("output/output_semaphore_generatedBooks.json", "w", encoding="utf8") as genstream:
                json.dump(dataset["Books"], genstream, indent=1)
