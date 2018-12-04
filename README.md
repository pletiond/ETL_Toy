# MI-PYT - Seminární práce

## Téma
### Představení
Výsledkem seminární práce bude jednoduchá CLI [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) aplikace.   
Hlavní funkčností bude dle uživatelem předem vytvořených scénářů provést ETL proces. Tvorba scénářů nahrávání bude možné přímo v jazyce Python, díky vytvořené knihovně. Po napsání scénáře bude kód možno transformovat do formátu JSON, s kterým bude pracovat aplikace při jeho provádění.  
Aplikace bude umět pracovat s **CSV** soubory(.csv), **Excel** soubory(.xls) a databází **PostgreSQL**.   
Podporovány budou nejpoužívanější datové typy - int, float, string, timestamp, bool. Možné zrychlení budu chtít otestovat použitím knihovny **NumPy**.   
Pro zjednodušení nebude možné proud dat dělit nebo slučovat a aplikace bude jedno vláknová. 
    
Transformační možnosti budou např:
* práce s textovými řetězci
* změna datového typu(nebo délky)
* lookup do jiného datového zdroje
* filtrace sloupců


### Funkční požadavky
* Nahrávání z/do souborů typu CSV, Excel nebo databáze PostgreSQL
* Tvorba vlastních scénářů a následné uložení ve formátu JSON
* Zapisování průběhu nahrávání do logu
* Bude možné provádět standartní transformace (přidání/odebrání sloupce, úprava dat. typu, práce s textem)

### Nefunkční požadavky
* Aplikace bude schopna pracovat se zdrojem s až 100 000 řádky
* Aplikace bude dostupná přes příkazovou řádku



