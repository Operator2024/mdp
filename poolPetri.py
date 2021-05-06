import json
from copy import copy, deepcopy
from multiprocessing import Pool, Manager
from os import cpu_count
from random import randint
from time import sleep, time
from typing import Any, NoReturn, Dict, Text, List


def worker(book: dict, author: Text, name: Text, q: Any, semaphore: Any = None,
           slp: float = 0) -> NoReturn:
    if author == book["Author"]:
        q.put([True, name, author])
    else:
        q.put([False])
    sleep(slp)
    if semaphore is not None:
        semaphore.put(1)


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
    PauseProcess: float = round(dataset["PT"] / 1000, 3)
    manager = Manager()
    qauthors = manager.Queue(MaxItemInStructure)

    pList = []
    ST_PARALLEL: float = round(time(), 1)
    PetriBooks: Dict = copy(dataset["Books"])

    PetriQueue = manager.Queue(MaxItemInStructure)

    # Количество позиций и переходов
    P = 4
    T = 3
    # Входное множество I
    I: List[List[int]] = [
        [ParallelThread, ParallelThread, 0, 0],
        [0, 0, 1, 0],
        [0, 0, 0, 1]]
    # Выходное множество O
    O: List[List[int]] = [
        [0, 0, ParallelThread, 0],
        [0, 0, 0, 1],
        [1, 0, 0, 0]]
    # Стартовое состояние
    U = [ParallelThread, MaxItemInStructure, 0, 0]
    # example U = [4, 5, 0, 0]

    print("=" * 10)
    print(f"Starting set (U) is equal [p1, p2, p3, p4] - {U}")
    print("_" * 5)

    sp = 0
    for i in I:
        sp += len(i)
    msg = "The work of the thread is over. Transition T3"
    msg_count = 0

    if sp / len(I) == P and len(I) == T:
        i = None
        with Pool(ParallelThread) as p:
            while True:
                if U[0] > 0 and U[1] > 0:
                    if I[0][0] >= 1 and I[0][1] >= 1:
                        for j in range(U[0]):
                            if len(PetriBooks) > 0:
                                for b in PetriBooks:
                                    i = b
                                PetriBooks.pop(i)
                                U[0] -= 1
                                U[1] -= 1
                                U[2] += 1
                                BookInfo = dataset["Books"][i]
                                BookName = i
                                BookAuthor = dataset["Books"][i]["Author"]

                                p.apply_async(func=worker, args=(
                                    BookInfo, keywriteAuthor, BookName, qauthors, PetriQueue,
                                    PauseProcess,))

                if PetriQueue.qsize() > 0:
                    token = PetriQueue.get()

                    if int(token) == I[1][2]:
                        if I[1][2] == O[1][3]:
                            U[0] += token
                    if int(token) == I[2][3]:
                        if U[2] > 0:
                            if I[2][3] == O[1][3]:
                                U[3] += I[2][3]
                                U[2] -= 1
                        if msg_count == 0:
                            print(msg)
                            msg_count += 1
                        else:
                            msg_count += 1
                if len(PetriBooks) == 0 and PetriQueue.qsize() == 0:
                    p.close()
                    p.join()
                    break

    # Check that the queue is empty. Move tokens from p3 to p4
    while True:
        if PetriQueue.qsize() > 0:
            token = PetriQueue.get()

            if int(token) == I[1][2]:
                if I[1][2] == O[1][3]:
                    U[0] += token
            if int(token) == I[2][3]:
                if U[2] > 0:
                    if I[2][3] == O[1][3]:
                        U[3] += I[2][3]
                        U[2] -= 1
                if msg_count == 0:
                    print(msg)
                    msg_count += 1
                else:
                    msg_count += 1
        else:
            if U[0] > 0:
                U[0] -= 1
            else:
                break

    print(msg, f"x {msg_count - 1}")

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

    print("_" * 5)
    print(f"Time of execution parallel - {ET_PARALLEL} (s)")
    print(f"The set U is equal now [p1, p2, p3, p4] - {U}\n")

    U = [ParallelThread, MaxItemInStructure, 0, 0]
    print(f"Starting set (U) is equal [p1, p2, p3, p4] - {U}")

    ST: float = round(time(), 1)
    for k in dataset["Books"]:
        if U[0] > 0 and U[1] > 0:
            if I[0][0] >= 1 and I[0][1] >= 1:
                U[0] -= 1
                U[1] -= 1
                U[2] += 1
                sleep(PauseProcess)
                BookInfo = dataset["Books"][k]
                BookName = k
                BookAuthor = dataset["Books"][k]["Author"]
                worker(BookInfo, keywriteAuthor, BookName, qauthors, PetriQueue, PauseProcess)
                if PetriQueue.qsize() > 0:
                    U[2] -= I[1][2]
                    U[3] += O[1][3]
                    U[0] += PetriQueue.get()

    while True:
        if U[0] > 0:
            U[0] -= 1
        else:
            break

    ET: float = round(time() - ST, 1)
    Result["T1"] = ET
    print(f"Time of execution successively - {ET} (s)")
    print(f"The set U is equal now [p1, p2, p3, p4] - {U}\n")
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

    with open("output/output_poolPetri.json", "w", encoding="utf8") as wr:
        json.dump(Result, wr, indent=1, ensure_ascii=False)
        print("Result -> ", Result)
        if dataset["N"] != NonGeneratedBooks:
            with open("output/output_poolPetri_generatedBooks.json", "w",
                      encoding="utf8") as genstream:
                json.dump(dataset["Books"], genstream, indent=1)
