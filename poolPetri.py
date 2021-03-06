import copy
import json
import multiprocessing
import os
from time import sleep, time
from typing import Dict, List, Text, Any, NoReturn


def worker(book: dict, author: Text, name: Text, slp: float = 0, q=None) -> NoReturn:
    # "Punishment Without Revenge": {"Author": "Lope de Vega", "Publisher": "Oberon Books", "Year": 1631, "Total pages": 96}
    # print(name)
    if author in book["Author"]:
        loc_list = [True, name, author]
    else:
        loc_list = [False]
    if q is not None:
        q.put(loc_list)
    sleep(slp)


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
        q_petri_token = multiprocessing.Queue(ParallelThread)

        lp: List = []
        ST_PARALLEL: float = round(time(), 1)
        # qauthors.put(pool.starmap(worker, [(dataset["Books"][i], keywriteAuthor, i, PauseProcess) for i in dataset["Books"]]))
        # pool.close()
        # pool.join()
        books_ = copy.copy(dataset["Books"])
        stopper: bool = False
        while True:
            if q_petri_token.qsize() < ParallelThread and stopper is not True:
                for b in books_.keys():
                    i = copy.copy(b)
                if len(books_) > 0:
                    books_.pop(i)
                else:
                    stopper = True
                if stopper is not True:
                    p = multiprocessing.Process(target=worker, args=(dataset["Books"][i], keywriteAuthor, i, PauseProcess, qauthors,))
                    p.start()
                    lp.append(p)
                    q_petri_token.put("1")
            elif (q_petri_token.qsize() > 0 or q_petri_token.qsize() == ParallelThread) or (q_petri_token.qsize() > 0 and stopper is True):
                while q_petri_token.qsize() > 0:
                    q_petri_token.get()
                    for p in lp:
                        p.join()
                        p.close()
                        lp.remove(p)
                if stopper is True:
                    break

        ET_PARALLEL: float = round(time() - ST_PARALLEL, 1)
        Result["TP"] = ET_PARALLEL
        print(f"Time of execution parallel - {ET_PARALLEL} (s)")
        while qauthors.qsize() > 0:
            if qauthors.qsize() > 0:
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
            book = dataset["Books"][k]
            book_name = k
            book_author = dataset["Books"][k]["Author"]
            worker(book, keywriteAuthor, book_name, slp=PauseProcess)
        ET: float = round(time() - ST, 1)
        Result["T1"] = ET
        print(f"Time of execution successively - {ET} (s)")

        with open("output/output_poolPetri.json", "w", encoding="utf8") as wr:
            json.dump(Result, wr, indent=1, ensure_ascii=False)
            print(Result)
    else:
        print("Total elements by key 'Books' in file input.json is not equal key 'N'!")
