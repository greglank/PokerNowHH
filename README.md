# PokerNowHH
Tools for converting and analyzing PokerNow hand histories. Primarily, a set of Python scripts that clean and parse PokerNow hand histories to load into a SQLite database for further analysis.

## This Project is Not (Yet) Ready for Prime Time

I am in the process of converting my PokerNow hand history parser to open source. It started as a personal project that was not intended to be shared widely, so I have some cleaning up to do! In the meantime, I have shared one of the Python scripts (stats.py) as a proof of concept. It is complete and functional, though not very meaningful without the other scripts.

For a description of the complete set of scripts, see my "Poker Hand History Parser and Database" portfolio page: https://greglank.github.io/parser

## stats.py

The Python script, stats.py, takes as input a SQLite database of no limit Texas hold 'em PokerNow hand histories created by history.py (upload to GitHub pending) and calculates a set of 'helper' variables for poker statistics. It modifies the database tables whose names begin with 'Stat' (primarily StatPlayerHands) to record these helper variables for hands and actions that do not yet have them.

The helper variables are usually boolean variables that indicate whether a hand or action meets the criteria for a particular poker statistic, such as voluntarily put money in pot (VPIP), continuation bet (cbet), etc. Adding these helper variables facilitates easier analysis through SQL queries and allows for more robust visualization through tools like Tableau.

The script calculates helper variables for all the poker statistics for no limit Texas hold 'em outlined in *The Grinder's Manual* by Peter Clarke, plus a few others that I created. These statistics are defined (with a page number reference to *The Grinder's Manual* where applicable) in comments within stats.py.

The script also outputs a smaller database with only the most recent hands (the default is the past 30 days) and with players' last names removed. This step is actually redundant with the sample database I have included here, because hh_sample.sqlite *is* the smaller database already. Normally this smaller database serves as the basis for the public Tableau dashboard described on my "Poker Visualization" portfolio page: https://greglank.github.io/visualization

### stats.py Usage

To use stats.py,
1. **Place stats.py in the same directory as hh_sample.sqlite and settings_default.json.**
2. **Rename settings_default.json to settings.json.** Optionally, you may edit settings.json to point to a different hand history database, or to multiple hand history databases, but it is best left unedited until I am able to publish the other scripts to GitHub.
3. **Run stats.py**. Use your favorite Python interpreter; see [python.org](https://www.python.org/about/gettingstarted/) for instructions.<br>
`python stats.py`

## Future Plans

As part of making this whole project open-source and available on GitHub, I plan to add a more robust set of database inputs and outputs. This includes a GUI for the database so users can look up queries without having to know SQL or the database structure, and a hand history converter that can output hands to a more common hand history format, like Poker Stars, that is more widely recognized by other software tools.

Here is an overview of the future plans for this project:
![Project plans](https://greglank.github.io/images/database-flowchart.jpg)