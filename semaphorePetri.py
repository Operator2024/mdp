import copy
import json
import multiprocessing
import os
from time import sleep, time
from typing import Text, Any, NoReturn, Dict


def worker(book: dict, author: Text, name: Text, q: Any, semaphore: Any = None, slp: float = 0) -> NoReturn:
    # "Punishment Without Revenge": {"Author": "Lope de Vega", "Publisher": "Oberon Books", "Year": 1631, "Total pages": 96}
    if author in book["Author"]:
        q.put([True, name, author])
    else:
        q.put([False])
    # print(semaphor)
    sleep(slp)
    if semaphore is not None:
        semaphore.put(1)


if __name__ == '__main__':
    with open("input.json", "r", encoding="utf8") as rw:
        dataset: Dict = json.load(rw)

    if dataset["N"] == len(dataset["Books"]):

        keywriteAuthor: Text = input("Enter the author of the book: ").rstrip(" ").lstrip("")

        ParallelThread: int = dataset["M"]
        MaxItemInStructure: int = dataset["N"]
        Result: Dict = dict()

        if ParallelThread == 0:
            ParallelThread = os.cpu_count()

        # Pause ms -> seconds
        PauseProcess: float = round(dataset["PT"] / 1000, 0)

        qauthors = multiprocessing.Queue(MaxItemInStructure)
        PetriToken = ParallelThread
        #
        pList = []
        ST_PARALLEL: float = round(time(), 1)
        PetriBooks = copy.copy(dataset["Books"])
        PetriQueue = multiprocessing.Queue(MaxItemInStructure)
        while True:
            if PetriToken > 0:
                for b in PetriBooks:
                    i = b
                PetriBooks.pop(i)
                BookInfo = dataset["Books"][i]
                BookName = i
                BookAuthor = dataset["Books"][i]["Author"]

                PetriToken -= 1
                p = multiprocessing.Process(target=worker, args=(BookInfo, keywriteAuthor, BookName, qauthors, PetriQueue, PauseProcess))
                p.start()
                pList.append(p)
            if PetriQueue.qsize() > 0:
                PetriToken += PetriQueue.get()
            if len(PetriBooks) == 0:
                break
            # sleep(PauseProcess)

        for j in pList:
            j.join()
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
        #
        ST: float = round(time(), 1)
        for k in dataset["Books"]:
            sleep(PauseProcess)
            BookInfo = dataset["Books"][k]
            BookName = k
            BookAuthor = dataset["Books"][k]["Author"]
            worker(BookInfo, keywriteAuthor, BookName, qauthors, slp=PauseProcess)
        ET: float = round(time() - ST, 1)
        Result["T1"] = ET

        print(f"Time of execution successively - {ET} (s)")

        with open("output/output_semaphorePetri.json", "w", encoding="utf8") as wr:
            json.dump(Result, wr, indent=1, ensure_ascii=False)
            print(Result)
    else:
        print("Total elements by key 'Books' in file input.json is not equal key 'N'!")
