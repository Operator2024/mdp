## Multithreading data processing 
The work was done according to the educational option twelfth task

Discipline: parallel programming

The main task of the work. Data processing from input.json file four different method:
1. [Semaphore](semaphore.py)
2. [Simulating a semaphore with a petri net](semaphorePetri.py)
3. [Thread pool](pool.py)
4. [Sumulation a thread pool with a petri net](poolPetri.py)

Input.json structure:
```json
{
  "PA": 1,
  "N": 5,
  "M": 2,
  "PT": 1000,
  "Books": {
    "Punishment Without Revenge": {"Author": "Lope de Vega", "Publisher":  "Oberon Books", "Year": 1631, "Total pages": 96},
    "The Master and Margarita": {"Author": "Mikhail Bulgakov", "Publisher":  "YMCA Press", "Year": 1967, "Total pages": 402},
    "Morphine": {"Author": "Mikhail Bulgakov", "Publisher":  "Noname", "Year": 1926, "Total pages": 333},
    "The Dog In The Manger": {"Author": "Lope de Vega", "Publisher":  "Oberon Books", "Year": 1618, "Total pages": 128},
    "Nineteen Eighty-Four": {"Author": "George Orwell", "Publisher":  "Secker & Warburg", "Year": 1949, "Total pages": 328}
  }
}
```
 * PA - Deprecated (Not used)
 * N - Total elements in 'Books' key
 * M - Total parallel threads
 * PT - Pause time in milliseconds
 * Books - Dataset for multithreading processing

## License

See the [LICENSE](LICENSE) file for license rights and limitations (MIT).

