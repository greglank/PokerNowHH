# PokerNowHH
Tools for converting and analyzing PokerNow hand histories. Primarily, a set of Python scripts that clean and parse PokerNow hand histories to load into a SQLite database for further analysis.

## This Project is Not (Yet) Ready for Prime Time

I am in the process of converting my PokerNow hand history tools to open source. This started as a personal project that was not intended to be shared widely, so I have some cleaning up to do! In the meantime, I have shared one of the Python scripts (stats.py) as a proof of concept. It is complete and functional, though not very meaningful without the other scripts.

To put this script in context, see my "Poker Hand History Parser and Database" portfolio page: https://greglank.github.io/parser

## stats.py

The Python script, stats.py, takes a SQLite database of no limit Texas hold 'em (NLHE) PokerNow hand histories created by history.py (upload to GitHub pending) and calculates a set of 'helper' variables for poker statistics. It modifies the database tables whose names begin with 'Stat' (primarily StatPlayerHands) to record these helper variables for players' hands that do not yet have them.

The helper variables are usually boolean variables that indicate whether a player's hand meets the criteria for a particular poker statistic, such as voluntarily put money in pot, continuation bet, etc. Adding these helper variables facilitates easier analysis through SQL queries and allows for more robust visualization through tools like Tableau.

The script calculates helper variables for all the poker statistics for NLHE outlined in *The Grinder's Manual* by Peter Clarke, plus a few others that I created. These statistics are defined in comments within stats.py, with a page number reference to *The Grinder's Manual* where applicable. Both cash games and tournaments are supported, but most of the stats are geared towards cash game play.

The script also outputs a smaller database with only the most recent hands (the default is 10 days, configurable in config.toml) and with players' last names removed. The sample database included here, hh_small.sqlite, is actually a small version of my full hand history database, going back one month with last names removed already. It serves as the basis for the public Tableau dashboard described on my "Poker Visualization" portfolio page: https://greglank.github.io/visualization

### stats.py Usage

To use stats.py,
1. **Place stats.py in the same directory as hh_small.sqlite and config_default.toml.** The database, hh_small.sqlite, is a sample hand history database included to make stats.py functional without the other scripts.
2. **Copy config_default.toml to config.toml.** Feel free to poke around in config.toml. The stats tables are already complete in the provided sample database; editing config.toml to set `clear_db = true` for the sample database will allow you to delete and re-create the stats tables. Optionally, you may edit config.toml to point to a different hand history database, or to multiple hand history databases. 
3. **Run stats.py**. Use your favorite Python interpreter (e.g. `python stats.py`); see [python.org](https://www.python.org/about/gettingstarted/) for instructions.

## Future Plans

As part of making this whole project open-source and available on GitHub, I plan to add a more robust set of database inputs and outputs. This includes a GUI for the database so users can look up queries without having to know SQL or the database structure, and a hand history converter that can output hands to a more common hand history format, like Poker Stars, that is more widely recognized by other software tools.

Here is an overview of the future plans for this project:
![Project plans](https://greglank.github.io/images/database-flowchart.jpg)
*Flowchart adapted from [Multi Input Output Process](https://poweredtemplate.com/multi-input-output-process-80158/)*
