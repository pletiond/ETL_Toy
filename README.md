# MI-PYT - Seminární práce

## Téma
### Představení
Výsledkem seminární práce bude jednoduchá CLI [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) aplikace.   
Hlavní funkčností bude dle uživatelem předem vytvořených scénářů provést ETL proces. Tvorba scénářů nahrávání bude možné přímo v jazyce Python, díky vytvořené knihovně. Po napsání scénáře bude kód možno transformovat do formátu JSON, s kterým bude pracovat aplikace při jeho provádění.  
Aplikace bude umět pracovat s **CSV** soubory(.csv), **Excel** soubory(.xls) a databází **PostgreSQL**.   
Podporovány budou nejpoužívanější datové typy - int, float, string, timestamp, bool. Možné zrychlení budu chtít otestovat použitím knihovny **NumPy**.   
Pro zjednodušení nebude možné proud dat dělit nebo slučovat a aplikace bude jednovláknová. 
    
Transformační možnosti budou např:
* práce s textovými řetězci
* změna datového typu(nebo délky)
* lookup do jiného datového zdroje
* filtrace sloupců

### Příklady scénářů

Uživatel bude chtít nahrát upravená data z CSV do databáze.

**CSV soubor:**   

user_bk | user_name | stud_id | age | sex
--- | --- | --- | --- | ---
1 | pletiond | 2131 | 23 | 1
2| friedmag | 2211 | 25 | 0
  
1. V nahrávacím  scénáři zvolí jako zdroj tento CSV soubor. (zdroj)
2. Přidá další krok pro úpravu sloupců a vybere sloupce user_bk, user_name, stud_id a sex. (filtrace sloupců)
3. Nastaví mapování u pohlaví: 0 -> M, 1 -> W (mapování)
4. Přidá krok pro lookup do databáze. A nastaví, že podle user_name najde v databázi peridno a udělá z toho nový sloupec. (lookup)
5. Přidá krok pro nahrávání dat do cílové databáze, nastaví připojení a mapování názvu sloupců. (nahrání)

**Příklad výstupu:**

user_bk | fk_osoba_peridno_bk | fk_stud_id_bk | sex
--- | --- | --- | --- 
1 | 231231 | 2131 |  M
2| 231299 | 2211 |  W

#### Ukázka jak by mohlo vypadat psaní scénáře:

```python
import etl_tool as  etl


transformation = etl.Transformation()

load  = etl.Load_from_csv()
load.set_path('tmp/my.csv')
load.load_all = True
load.auto_set_data_types = True

transformation.add_step(load)

select = etl.SelectColumns()
select.columns = ['user_bk', 'user_name', 'stud_id', 'sex']

transformation.add_step(select)

mapping = etl.MapValues()
mapping.rules ={'sex':[[0,'W'],[1, 'M']}

transformation.add_step(mapping)

...

transformation.save('tmp/transformation.json')


```
Výsledký soubor bude použit pro nahrávací aplikaci, která provede ETL.


### Funkční požadavky
* Nahrávání z/do souborů typu CSV, Excel nebo databáze PostgreSQL
* Tvorba vlastních scénářů a následné uložení ~~ve formátu JSON~~
* Zapisování průběhu nahrávání do logu
* Bude možné provádět standartní transformace (přidání/odebrání sloupce, úprava dat. typu, práce s textem)

### Nefunkční požadavky
* Aplikace bude schopna pracovat se zdrojem s až 100 000 řádky
* Aplikace bude dostupná přes příkazovou řádku



