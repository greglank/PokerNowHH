# -*- coding: utf-8 -*-
"""
Created on Thu Jun  9 11:01:12 2022

@author: greglank

Script takes a SQLite database of no limit Texas hold 'em (NLHE) PokerNow hand histories created by history.py
and calculates a set of 'helper' variables for poker statistics. It modifies the database tables whose names begin
with 'Stat' (primarily StatPlayerHands) to record these helper variables for hands that do not yet have them.
The script also outputs a smaller database with only the most recent hands and with players' last names removed.
Uses config.toml for configuration.
"""

"""
Wish list (XXX):
-Calculate only new columns (instead of resetting entire db):
    1. Add boolean variable in calc methods to use NewActions vs Actions
    2. Use conditional ALTER TABLE for new columns
    -set boolean variable (e.g. list of column names) here
-Remove duplicate sess_num columns?
-Won w/o SD % is sometimes negative in Tableau: (COUNT([Wwsf])-COUNT([Won Sd]))/COUNT([N Wwsf])
...Likely related to how wwsf/n_wwsf only counts when players take an action on flop, not when they are active
...(e.g. they could have gone all-in preflop)
-Delete tourney tables from small db
-Run it twice winning hand for second board (needs to be set in history.py first)
-Duplicate HH file (or duplicate player names) creates redundant entries in finisher list
...(unique constraint fails when adding StatTourneyPlaces.sess_num and StatTourneyPlaces.player_id)    
"""

import sqlite3  # sqlite database
import shutil  # file copy
import tomllib  # toml config file
import time  # pause

# import config file
with open('config.toml', mode='rb') as f:
    config = tomllib.load(f)

TEST_RUN = config['test_run']
TIME_DIFF = config['time_diff']
if TEST_RUN:
    SMALL_DB_NAME = config['small_db']['out_name_test']
else:
    SMALL_DB_NAME = config['small_db']['out_name']
SMALL_DAYS = config['small_db']['num_days']
DB_LIST = config['db_list']  # list of database names and settings
# CONST = config['const']  # list of constants shared across scripts

# actions for database
POST_VAL = config['const']['POST_VAL']
POST_MISSING_VAL = config['const']['POST_MISSING_VAL']  # missing blind post, not credited to player
POST_MISSED_VAL = config['const']['POST_MISSED_VAL']  # missed blind post, credited to player
FOLD_VAL = config['const']['FOLD_VAL']
CHECK_VAL = config['const']['CHECK_VAL']
CALL_VAL = config['const']['CALL_VAL']
BET_VAL = config['const']['BET_VAL']
RAISE_VAL = config['const']['RAISE_VAL']

# tournament actions for database
QUIT_VAL = config['const']['QUIT_VAL']
BUYIN_VAL = config['const']['BUYIN_VAL']
REBUY_VAL = config['const']['REBUY_VAL']

# streets for database
PREFLOP_VAL = config['const']['PREFLOP_VAL']
FLOP_VAL = config['const']['FLOP_VAL']  # flop=3 is more human readable
TURN_VAL = config['const']['TURN_VAL']
RIVER_VAL = config['const']['RIVER_VAL']
SHOWDOWN_VAL = config['const']['SHOWDOWN_VAL']

# blind values to add to position; SB is bigger because it's the worst position
STRADDLE_VAL = config['const']['STRADDLE_VAL']
BB_VAL = config['const']['BB_VAL']
SB_VAL = config['const']['SB_VAL']

# minimum and maximum positions at table
POS_MIN = config['const']['POS_MIN']  # button
POS_MAX = config['const']['POS_MAX']  # utg at 10-player table


def try_query(cur, query, values=None):
    '''Tries to execute query and catches exception if it fails.
    Useful for hacky equivalent to ALTER TABLE IF NOT EXISTS
    and for troubleshooting pesky DB errors.'''
    
    try:
        if values:
            cur.execute(query, values)
        else:
            cur.execute(query)
        # print(f'Success: {query}')
    except sqlite3.OperationalError:
        # adding column that already exists throws sqlite3.OperationalError
        # so does referencing a table that does not exist in a subquery
        err = 'OperationalError'
        print(f'...OperationalError: {query}')
    except sqlite3.IntegrityError:
        # failing a UNIQUE constraing throws sqlite3.IntegrityError
        err = 'IntegrityError'
        print(f'...IntegrityError: {query}')
    else:
        # no exception
        err = None

    return err

def calc_action(conn, stat_name, stat_cond, stat_cond2='',
                val='1', join_hands=False, join_p_hands=False):
    '''General DB query for single actions.
    Fills in value (val) for a column (stat_name) that matches (stat_cond).
    By default, uses val=1 for boolean values.
    Also checks optional condition (stat_cond2) for previous boolean values.
    If optional (join_hands) or (join_p_hands) flags are set to true,
    JOINS Hands/PlayerHands table for hand info.'''
    
    '''
    The general form of the subquery is
     SELECT table_id, hand_num, player_id
     FROM NewActions [JOIN Hands/PlayerHands USING (blah)]
     WHERE conditions
    The general form of the query is
     UPDATE StatPlayerHands
     SET stat_name = value <default is 1 for boolean true>
     WHERE (table_id, hand_num, player_id) IN
     (subquery)
     [AND optional condition relating to StatPlayerHands]
    '''
    
    # Optional stat_cond2 checks previous boolean values
    # (e.g., checking for existence of cbets on previous streets)
    if stat_cond2:
        cond_str = f' AND {stat_cond2}'
    else:
        cond_str = ''
    
    # Optional JOIN to Hands/PlayerHands table for hand info
    if join_hands:
        join_str = 'JOIN Hands USING (table_id, hand_num) '
    elif join_p_hands:
        join_str = 'JOIN PlayerHands USING (table_id, hand_num, player_id) '
    else:
        join_str = ''

    cur = conn.cursor()
    query = ('UPDATE StatPlayerHands '
             + f'SET {stat_name} = {val} '
             + 'WHERE (table_id, hand_num, player_id) IN '
             + '(SELECT table_id, hand_num, player_id '
             + f'FROM NewActions {join_str}'
             + f'WHERE {stat_cond})'
             + f'{cond_str}')
    print(stat_name, end=' ')
    # print(query)
    cur.execute(query)
            
    conn.commit()
    cur.close()


def calc_seq_action(conn, stat_name, stat_cond, same_player=True, join_hands=False):
    '''General DB query for sequential actions on same street.
    Fills in boolean values for a stat column (stat_name)
    that matches (stat_cond).
    In (stat_cond), use <a1.x> and <a2.x> for action1 and action2, respectively.
    Use (same_player) flag for same/different players for action1 and action2.
    If optional (join_hands) is set to true, JOINS Hands table for hand info.'''

    '''
    The general form of the subquery is
     SELECT table_id, hand_num, a2.player_id
     FROM NewActions a1 JOIN NewActions a2 USING (blah)
     [JOIN Hands USING (blah)]
     WHERE a1.action_num < a2.action_num [AND a1.player_id = a2.player_id]
     AND conditions
    The general form of the query is the same as calc_action
    '''
    
    # same or different player for action1 and action2
    if same_player:
        player_str = 'AND a1.player_id = a2.player_id '
    else:
        player_str = ''
    
    # optional JOIN to Hands table for hand info
    if join_hands:
        join_str = 'JOIN Hands USING (table_id, hand_num) '
    else:
        join_str = ''

    cur = conn.cursor()
    query = ('UPDATE StatPlayerHands '
             + f'SET {stat_name} = 1 '
             + 'WHERE (table_id, hand_num, player_id) IN '
             + '(SELECT table_id, hand_num, a2.player_id '
             + 'FROM NewActions a1 JOIN NewActions a2 '
             + 'USING (table_id, hand_num, street) '
             + f'{join_str}'
             + f'WHERE a1.action_num < a2.action_num {player_str}'
             + f'AND {stat_cond})')
    print(stat_name, end=' ')
    # print(query)
    cur.execute(query)
            
    conn.commit()
    cur.close()


def set_value(conn, col, val, subq, values=None,
              match_player=True, match_row=False, null_only=True):
    '''Sets value (val) for column (col) based on results of a subquery (subq)
    that uses a different table.
    For (val), columns must be in the form of <other.col>.
    Optional boolean (match_player) matches table_id, hand_num, and player_id;
    otherwise only table_id is matched.
    Optional boolean (match_row) replaces table_id with row_num and player_id.
    Optional boolean (null_only) only sets rows where (col) is null.'''
    
    '''This is a huge pain to test; the query does not work in DB Browser for SQLite.'''
    
    '''
    The general form of the subquery is
     SELECT blah from SecondaryTable
    The general form of the query is
     UPDATE PrimaryTable
     SET col_primary = alias.col_secondary
     FROM (subquery) AS alias
     WHERE PrimaryTable.id = alias.id
    '''
    
    cur = conn.cursor()
           
    # only set rows where (col) is null; i.e. there is no existing value
    if null_only:
        null_str = f' AND StatPlayerHands.{col} IS NULL'
    else:
        null_str = ''
    
    query = ('UPDATE StatPlayerHands'
             + f' SET {col} = {val}'
             + f' FROM ({subq}) AS other')
    
    if match_row:
        query += (' WHERE StatPlayerHands.row_num = other.row_num'
                  + ' AND StatPlayerHands.player_id = other.player_id')
    else:
        query += ' WHERE StatPlayerHands.table_id = other.table_id'
        if match_player:
            query += (' AND StatPlayerHands.hand_num = other.hand_num'
                      + ' AND StatPlayerHands.player_id = other.player_id')
            
    query += f'{null_str}'
    
    print(col, end=' ')
    # print(query)
    if values is None:
        cur.execute(query)
    else:
        cur.execute(query, values)
    
    conn.commit()
    cur.close()


def count_action(conn, stat_name, stat_cond):
    '''General DB query for counting number of actions taken by player in a hand.
    Fills in count values for a column (stat_name) that matches (stat_cond).'''
    
    '''I actually came up with this before set_value() and didn't realize that
    it has the same general query form. Could actually call set_value() with
    the counting subquery as the (subq) argument.'''

    '''
    The general form of the subquery is
     SELECT table_id, hand_num, player_id, COUNT(*) num_actions
     FROM NewActions JOIN Hands USING (blah)
     WHERE conditions
     GROUP BY table_id, hand_num, player_id
    The general form of the query is the same as set_value
    '''

    cur = conn.cursor()
    subquery = ('SELECT table_id, hand_num, player_id, COUNT(*) num_actions'
                + ' FROM NewActions JOIN Hands USING (table_id, hand_num)'
                + f' WHERE {stat_cond}'
                + ' GROUP BY table_id, hand_num, player_id')
    query = ('UPDATE StatPlayerHands'
             + f' SET {stat_name} = sq.num_actions'
             + f' FROM ({subquery}) AS sq'
             + ' WHERE StatPlayerHands.table_id = sq.table_id'
             + ' AND StatPlayerHands.hand_num = sq.hand_num'
             + ' AND StatPlayerHands.player_id = sq.player_id')
    print(stat_name, end=' ')
    # print(query)
    cur.execute(query)
            
    conn.commit()
    cur.close()


def split_sessions(conn, num_days=0, num_sessions=0, return_all=False):
    ''' Finds the beginning time of each session within the supplied constraints.
    Returns the result of a query that finds the first hand of each session
    (sessions defined by >= TIME_DIFF days between hands) from newest to oldest.
    Use num_sessions and/or num_days to limit number of returned sessions.
    By default, the OLDEST date is the last result.
    Use return_all=True to return all results (as a query) instead of only
    the oldest result (as a string date).'''

    # query that returns the first hand of each session
    # (sessions defined by >= 1 day between hands)
    # orders by most recent sessions first

    # if return_all:
    # if returning all results, also return session number
    # adds +1 because first session is skipped
    query = 'SELECT *, (DENSE_RANK() OVER (ORDER by time)) + 1 AS sn FROM'
    # else:
    #    query = 'SELECT * FROM'

    # replaced 'now' with most recent hand time: (SELECT MAX(time) from TableNames)
    # should select the FIRST hand of the most recent session, not the last XXX
    query += '''(SELECT t2.time, t2.table_id, t2.hand_num,
                 julianday(t2.time) - julianday(t1.time) AS time_diff,
                 julianday((SELECT MAX(time) from TableNames)) - julianday(t2.time) AS time_ago
                FROM
                    (SELECT DENSE_RANK() OVER (ORDER BY time) rn,
                     time, table_id, hand_num
                    FROM Hands) t1
                JOIN
                    (SELECT DENSE_RANK() OVER (ORDER BY time) rn,
                     time, table_id, hand_num
                    FROM Hands) t2
                ON t2.rn = t1.rn + 1)
            WHERE time_diff >= ?'''

    recent_cur = conn.cursor()
    begin_time = None

    values = (TIME_DIFF,)

    # limit by days ago
    if num_days > 0:
        query += ' AND time_ago <= ?'
        values += (num_days,)

    query += ' ORDER BY time DESC'

    # limit by number of sessions
    if num_sessions > 0:
        query += ' LIMIT ?'
        values += (num_sessions,)

    recent_cur.execute(query, values)

    if return_all:
        return recent_cur

    print('Sessions:')

    # for idx in range(1, num_sessions + 1):
    # begin_time = recent_cur.fetchone()[0]
    for row in recent_cur:
        begin_time = row[0]
        print(begin_time)
        # sess_num = row[5]
        # print(row)
        # print(f'#{sess_num}: {begin_time}')

    if begin_time is None:
        # nothing returned usually means there is only one session
        # (e.g., using test db)
        print('None returned')
        query = '''SELECT MIN(time)
                    FROM Hands'''
        recent_cur.execute(query)
        (begin_time,) = recent_cur.fetchone()
        print(begin_time)

    print(f'Returning begin date: {begin_time}')
    return begin_time


def create_stats_table_sparse(conn, db_name, clear_db):
    '''Creates StatPlayerHands table
    Wish list: compute stats for new columns only XXX'''

    cur = conn.cursor()

    # delete existing databases
    # all tourney stat DBs need to be deleted because sess_num is dynamic
    cur.execute('DROP TABLE IF EXISTS StatTourneyPlaces')
    cur.execute('DROP TABLE IF EXISTS StatTourneyHands')

    if TEST_RUN or clear_db:
        db_delete = 'y'
        if clear_db:
            db_delete = input(f'Clear existing stats tables for {db_name}? (Y/n): ')
        if db_delete.lower() == 'y' or db_delete == '':
            cur.execute('DROP TABLE IF EXISTS StatPlayerHands')
            cur.execute('DROP TABLE IF EXISTS StatPvP')
            # reduce file size
            conn.commit()
            conn.execute('VACUUM')
            conn.commit()

    # create StatPlayerHands (see run_ methods for stat descriptions)
    query = '''CREATE TABLE IF NOT EXISTS StatPlayerHands
        (row_num INTEGER,

         table_id INTEGER,
         hand_num INTEGER,
         player_id INTEGER,

         date_added TIMESTAMP(3),

         vpip BOOLEAN,
         pfr BOOLEAN,
         n_threebet BOOLEAN,
         threebet BOOLEAN,
         n_fourbet BOOLEAN,
         fourbet BOOLEAN,
         n_face3bet BOOLEAN,
         rr3bet BOOLEAN,
         call3bet BOOLEAN,
         foldto3bet BOOLEAN,
         n_face4bet BOOLEAN,
         foldto4bet BOOLEAN,
         n_callopen BOOLEAN,
         callopen BOOLEAN,

         n_cbet_flop BOOLEAN,
         cbet_flop BOOLEAN,
         n_cbet_turn BOOLEAN,
         cbet_turn BOOLEAN,
         n_cbet_river BOOLEAN,
         cbet_river BOOLEAN,
         n_faced_cbet_flop BOOLEAN,
         foldto_cbet_flop BOOLEAN,
         raise_cbet_flop BOOLEAN,
         n_faced_3cbet_flop BOOLEAN,
         foldto_3cbet_flop BOOLEAN,
         n_faced_cbet_turn BOOLEAN,
         foldto_cbet_turn BOOLEAN,
         n_faced_cbet_river BOOLEAN,
         foldto_cbet_river BOOLEAN,

         n_stab_flop BOOLEAN,
         stab_flop BOOLEAN,
         n_stab_turn BOOLEAN,
         stab_turn BOOLEAN,
         n_stab_river BOOLEAN,
         stab_river BOOLEAN,
         n_donk_flop BOOLEAN,
         donk_flop BOOLEAN,
         n_donk_turn BOOLEAN,
         donk_turn BOOLEAN,
         n_donk_river BOOLEAN,
         donk_river BOOLEAN,

         n_rfi BOOLEAN,
         rfi BOOLEAN,
         n_rfi_1 BOOLEAN,
         rfi_1 BOOLEAN,
         n_rfi_2 BOOLEAN,
         rfi_2 BOOLEAN,
         n_rfi_3 BOOLEAN,
         rfi_3 BOOLEAN,
         n_rfi_4 BOOLEAN,
         rfi_4 BOOLEAN,
         n_rfi_5 BOOLEAN,
         rfi_5 BOOLEAN,
         n_rfi_6 BOOLEAN,
         rfi_6 BOOLEAN,
         n_rfi_7 BOOLEAN,
         rfi_7 BOOLEAN,
         n_rfi_8 BOOLEAN,
         rfi_8 BOOLEAN,
         n_rfi_bb BOOLEAN,
         rfi_bb BOOLEAN,
         n_rfi_sb BOOLEAN,
         rfi_sb BOOLEAN,

         n_wwsf BOOLEAN,
         wwsf BOOLEAN,
         won_sd BOOLEAN,
         n_show BOOLEAN,
         show BOOLEAN,

         stack_bb FLOAT,
         balance_bb FLOAT,
         bal_bb_running FLOAT,

         n_limpr BOOLEAN,
         limpr BOOLEAN,
         n_cr_flop BOOLEAN,
         cr_flop BOOLEAN,
         n_cr_turn BOOLEAN,
         cr_turn BOOLEAN,
         n_cr_river BOOLEAN,
         cr_river BOOLEAN,
         n_cr_cbet_flop BOOLEAN,
         cr_cbet_flop BOOLEAN,
         n_cr_cbet_any BOOLEAN,
         cr_cbet_any BOOLEAN,

         n_actions INTEGER DEFAULT 0,
         n_folds INTEGER DEFAULT 0,
         n_checks INTEGER DEFAULT 0,
         n_calls INTEGER DEFAULT 0,
         n_bets INTEGER DEFAULT 0,
         n_raises INTEGER DEFAULT 0,
         n_check_rs INTEGER DEFAULT 0,

         FOREIGN KEY(table_id) REFERENCES TableNames(table_id) ON UPDATE CASCADE,
         FOREIGN KEY(player_id) REFERENCES PlayerNames(player_id) ON UPDATE CASCADE,
         UNIQUE(table_id, hand_num, player_id)
        )'''
    cur.execute(query)

    query = '''INSERT OR IGNORE INTO StatPlayerHands
        (table_id, hand_num, player_id)
        SELECT table_id, hand_num, player_id
        FROM PlayerHands'''
    cur.execute(query)

    # create StatPvP table
    query = '''CREATE TABLE IF NOT EXISTS StatPvP
        (table_id INTEGER,
         hand_num INTEGER,
         player_id INTEGER,

         opp_id INTEGER,
         net_chips INTEGER,
         net_bb FLOAT,

         FOREIGN KEY(table_id) REFERENCES TableNames(table_id) ON UPDATE CASCADE,
         FOREIGN KEY(player_id) REFERENCES PlayerNames(player_id) ON UPDATE CASCADE,
         FOREIGN KEY(opp_id) REFERENCES PlayerNames(player_id) ON UPDATE CASCADE,
         UNIQUE(table_id, hand_num, player_id, opp_id)
        )'''
    cur.execute(query)

    # create StatTourneyPlaces table
    query = '''CREATE TABLE IF NOT EXISTS StatTourneyPlaces
        (sess_num INTEGER,
         place INTEGER,
         player_id INTEGER,
         FOREIGN KEY(sess_num) REFERENCES TableNames(sess_num) ON UPDATE CASCADE,
         FOREIGN KEY(player_id) REFERENCES PlayerNames(player_id) ON UPDATE CASCADE,
         UNIQUE(sess_num, player_id)
        )'''
    cur.execute(query)

    # create StatTourneyHands table
    query = '''CREATE TABLE IF NOT EXISTS StatTourneyHands
        (sess_num INTEGER,
         sess_hand INTEGER,
         table_id INTEGER,
         hand_num INTEGER,
         total_stacks INTEGER,
         total_players INTEGER,
         FOREIGN KEY(table_id) REFERENCES TableNames(table_id) ON UPDATE CASCADE,
         UNIQUE(table_id, hand_num)
        )'''
    cur.execute(query)

    conn.commit()
    cur.close()


def create_new_actions_table(conn):
    '''Creates temporary table of new actions'''

    cur = conn.cursor()

    # select actions from hands that have not yet been added
    # currently checks for null; check for older date XXX
    print('Adding new actions to database...')
    query = '''CREATE TEMPORARY TABLE IF NOT EXISTS NewActions AS
        SELECT Actions.*
        FROM Actions JOIN StatPlayerHands USING (table_id, hand_num, player_id)
        WHERE StatPlayerHands.date_added IS NULL'''
    cur.execute(query)

    # calculate number of newly-added actions
    query = 'SELECT COUNT(*) FROM NewActions'
    cur.execute(query)
    num_new = cur.fetchone()[0]
    print(f'Number of new actions added: {num_new}')

    conn.commit()
    cur.close()

    return num_new


def create_new_hands_table(conn):
    '''Creates temporary table of new hands'''

    cur = conn.cursor()

    # select hands that have not yet been added
    # queries NewActions
    print('Adding new hands to database...')
    query = '''CREATE TEMPORARY TABLE IF NOT EXISTS NewHands AS
        SELECT table_id, hand_num
        FROM NewActions
        GROUP BY table_id, hand_num'''
    cur.execute(query)

    # calculate number of newly-added hands
    query = 'SELECT COUNT(*) FROM NewHands'
    cur.execute(query)
    num_new = cur.fetchone()[0]
    print(f'Number of new hands added: {num_new}')

    conn.commit()
    cur.close()


def run_preflop_stats(conn):
    '''Helper method for calling each of the preflop stats methods.'''
    
    # VPIP (any preflop call, bet, or raise)
    # p. 29
    stat_cond = f'street = {PREFLOP_VAL} AND action_id >= {CALL_VAL}'
    calc_action(conn, 'vpip', stat_cond)
    
    # PFR (any preflop raise)
    stat_cond = f'street = {PREFLOP_VAL} AND action_id = {RAISE_VAL}'
    calc_action(conn, 'pfr', stat_cond)
    
    # All potential 3-bets (preflop pot has been raised exactly once)
    base_cond = f'street = {PREFLOP_VAL} AND bet_level = 2'
    calc_action(conn, 'n_threebet', base_cond)
    
    # 3-bet (preflop raise when pot has already been raised exactly once)
    # p. 52
    stat_cond = base_cond + f' AND action_id = {RAISE_VAL}'
    calc_action(conn, 'threebet', stat_cond)
    
    # All potential 4-bets (preflop pot has been raised exactly twice)
    base_cond = f'street = {PREFLOP_VAL} AND bet_level = 3'
    calc_action(conn, 'n_fourbet', base_cond)
    
    # 4-bet (preflop raise when pot has already been raised exactly twice)
    # Low/med/high assumed to be 3-bet * 2/3 (2%/5%/8%)
    stat_cond = base_cond + f' AND action_id = {RAISE_VAL}'
    calc_action(conn, 'fourbet', stat_cond)
        
    # All facing a 3-bet after open (preflop raise, then facing one re-raise)
    base_cond = (f'street = {PREFLOP_VAL} AND a1.bet_level <= 1'
                 + f' AND a1.action_id = {RAISE_VAL} AND a2.bet_level = 3')
    calc_seq_action(conn, 'n_face3bet', base_cond)
    
    # Reraise 3-bet ("4-bet after open")
    stat_cond = base_cond + f' AND a2.action_id = {RAISE_VAL}'
    calc_seq_action(conn, 'rr3bet', stat_cond)

    # Call 3-bet after open (mostly just a sanity check for rr/fold numbers)
    stat_cond = base_cond + f' AND a2.action_id = {CALL_VAL}'
    calc_seq_action(conn, 'call3bet', stat_cond)
    
    # Fold to 3-bet after open
    # p. 327
    stat_cond = base_cond + f' AND a2.action_id = {FOLD_VAL}'
    calc_seq_action(conn, 'foldto3bet', stat_cond)
        
    # All facing a 4-bet after 3-bet (preflop 3-bet, then facing one re-raise)
    base_cond = (f'street = {PREFLOP_VAL} AND a1.bet_level = 2'
                 + f' AND a1.action_id = {RAISE_VAL} AND a2.bet_level = 4')
    calc_seq_action(conn, 'n_face4bet', base_cond)
    
    # Fold to 4-bet after 3-bet
    # p. 378
    stat_cond = base_cond + f' AND a2.action_id = {FOLD_VAL}'
    calc_seq_action(conn, 'foldto4bet', stat_cond)
    
    # Fold vs button open XXX
    # p. 47
    
    # Fold vs SB open XXX
    # p. 62
    
    # All possible call pf open (fisrt caller, excludes BB)
    base_cond = (f'street = {PREFLOP_VAL} AND bet_level = 2 AND n_commit <= 1'
                 + f' AND (pos < {BB_VAL} OR pos >= {SB_VAL})')
    calc_action(conn, 'n_callopen', base_cond, join_p_hands=True)
    
    # Call pf open (first caller, excludes BB)
    stat_cond = base_cond + f' AND action_id = {CALL_VAL}'
    calc_action(conn, 'callopen', stat_cond, join_p_hands=True)
    
        
def run_cbet_stats(conn):
    '''Helper method for calling each of the cbet stats methods.'''
    
    # All potential cbets on flop (opportunity to open flop as preflop aggressor)
    base_cond = f'street = {FLOP_VAL} AND bet_level = 0 AND player_id = pf_agg_id'
    calc_action(conn, 'n_cbet_flop', base_cond, join_hands=True)
    
    # Cbet flop (open flop as preflop aggressor)
    # p. 222
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    calc_action(conn, 'cbet_flop', stat_cond, join_hands=True)

    # All potential cbets on turn (opportunity to open turn as flop aggressor)
    base_cond = f'street = {TURN_VAL} AND bet_level = 0 AND player_id = flop_agg_id'
    # base_cond = (f'street = {TURN_VAL} AND bet_level = 0 AND player_id = pf_agg_id'
    #             + ' AND player_id = flop_agg_id')
    calc_action(conn, 'n_cbet_turn', base_cond, join_hands=True)

    # Cbet turn (open turn as flop aggressor)
    # p. 223
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    calc_action(conn, 'cbet_turn', stat_cond, join_hands=True)

    # All potential cbets on river (opportunity to open river as turn aggressor)
    base_cond = f'street = {RIVER_VAL} AND bet_level = 0 AND player_id = turn_agg_id'
    # base_cond = (f'street = {RIVER_VAL} AND bet_level = 0 AND player_id = pf_agg_id'
    #             + ' AND player_id = flop_agg_id AND player_id = turn_agg_id')
    calc_action(conn, 'n_cbet_river', base_cond, join_hands=True)

    # Cbet river (open river as turn aggressor)
    # p. 224
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    calc_action(conn, 'cbet_river', stat_cond, join_hands=True)
    
    # All cbets faced on flop (facing opening flop bet by preflop aggressor)
    base_cond = f'street = {FLOP_VAL} AND bet_level = 1 AND agg_id = pf_agg_id'
    calc_action(conn, 'n_faced_cbet_flop', base_cond, join_hands=True)
    
    # Fold to flop cbet (fold to opening flop bet by preflop aggressor)
    # p. 56
    stat_cond = base_cond + f' AND action_id = {FOLD_VAL}'
    calc_action(conn, 'foldto_cbet_flop', stat_cond, join_hands=True)

    # Raise flop cbet (raise opening flop bet by preflop aggressor)
    # p. 104
    stat_cond = base_cond + f' AND action_id = {RAISE_VAL}'
    calc_action(conn, 'raise_cbet_flop', stat_cond, join_hands=True)
    
    # All cbets faced on flop from preflop 3-bettor
    base_cond = base_cond + ' AND pf_bet_level = 3'
    calc_action(conn, 'n_faced_3cbet_flop', base_cond, join_hands=True)
    
    # Fold to flop cbet from preflop 3-bettor
    # Low/med/high assumed to be fold to flop cbet + 5% (42.5%/55%/67.5%)
    stat_cond = base_cond + f' AND action_id = {FOLD_VAL}'
    calc_action(conn, 'foldto_3cbet_flop', stat_cond, join_hands=True)

    '''What defines a cbet on the turn or river? In increasing strictness:
        1. Opening bet as the aggressor of the previous street
        2. Opening bet as the aggressor of ALL previous streets
        3. Opening bet after cbetting ALL previous streets (not just any aggression;
           has to meet the definition of a cbet)
    The previous version of this program used interpretation #2.
    Then I used interpretation #3, which is most in line with how this
    stat can be useful (e.g., will my opponent fold if I keep cbetting?).
    This necessitated a check for cbet criteria on previous streets,
    which made the DB query method (calc_action) more complicated.
    Now using criteria #1 because there were some conspicuous absences in
    the stats, and this is still useful for determining whether an opponent
    will fold to repeated aggression.
    Note: Grinder's Manual uses interpretation #3.'''

    # All cbets faced on turn (facing opening turn bet by hand aggressor)
    base_cond = f'street = {TURN_VAL} AND bet_level = 1 AND agg_id = flop_agg_id'
    calc_action(conn, 'n_faced_cbet_turn', base_cond, join_hands=True)
    # base_cond = (f'street = {TURN_VAL} AND bet_level = 1 AND agg_id = pf_agg_id'
    #              + ' AND agg_id = flop_agg_id')
    # base_cond2 = 'n_faced_cbet_flop = 1'
    # calc_action(conn, 'n_faced_cbet_turn', base_cond, base_cond2, join_hands=True)
    
    # Fold to turn cbet (fold to opening turn bet by hand aggressor)
    stat_cond = base_cond + f' AND action_id = {FOLD_VAL}'
    calc_action(conn, 'foldto_cbet_turn', stat_cond, join_hands=True)
    # calc_action(conn, 'foldto_cbet_turn', stat_cond, base_cond2, join_hands=True)

    # All cbets faced on river (facing opening river bet by hand aggressor)
    base_cond = f'street = {RIVER_VAL} AND bet_level = 1 AND agg_id = turn_agg_id'
    calc_action(conn, 'n_faced_cbet_river', base_cond, join_hands=True)
    # base_cond = (f'street = {RIVER_VAL} AND bet_level = 1 AND agg_id = pf_agg_id'
    #             + ' AND agg_id = flop_agg_id AND agg_id = turn_agg_id')
    # base_cond2 = 'n_faced_cbet_flop = 1 AND n_faced_cbet_turn = 1'
    # calc_action(conn, 'n_faced_cbet_river', base_cond, base_cond2, join_hands=True)
                
    # Fold to river cbet (fold to opening river bet by hand aggressor)
    stat_cond = base_cond + f' AND action_id = {FOLD_VAL}'
    calc_action(conn, 'foldto_cbet_river', stat_cond, join_hands=True)
    # calc_action(conn, 'foldto_cbet_river', stat_cond, base_cond2, join_hands=True)


def run_stab_stats(conn):
    '''Helper method for calling each of the stab/donk stats methods.'''
    
    # All potential flop stabs (opp to open flop after preflop aggressor checked)
    base_cond = (f'street = {FLOP_VAL} AND a1.player_id = pf_agg_id'
                 + f' AND a1.action_id = {CHECK_VAL} AND a2.bet_level = 0')
    calc_seq_action(conn, 'n_stab_flop', base_cond, same_player=False, join_hands=True)
    
    # Stab flop (open flop after preflop aggressor checked)
    # p. 146
    stat_cond = base_cond + f' AND a2.action_id = {BET_VAL}'
    calc_seq_action(conn, 'stab_flop', stat_cond, same_player=False, join_hands=True)

    # All potential turn stabs (opp to open turn after flop aggressor checked)
    base_cond = (f'street = {TURN_VAL} AND a1.player_id = flop_agg_id'
                 + f' AND a1.action_id = {CHECK_VAL} AND a2.bet_level = 0')
    calc_seq_action(conn, 'n_stab_turn', base_cond, same_player=False, join_hands=True)
    
    # Stab turn (open turn after flop aggressor checked)
    stat_cond = base_cond + f' AND a2.action_id = {BET_VAL}'
    calc_seq_action(conn, 'stab_turn', stat_cond, same_player=False, join_hands=True)

    # All potential river stabs (opp to open river after turn aggressor checked)
    base_cond = (f'street = {RIVER_VAL} AND a1.player_id = turn_agg_id'
                 + f' AND a1.action_id = {CHECK_VAL} AND a2.bet_level = 0')
    calc_seq_action(conn, 'n_stab_river', base_cond, same_player=False, join_hands=True)
    
    # Stab river (open river after turn aggressor checked)
    stat_cond = base_cond + f' AND a2.action_id = {BET_VAL}'
    calc_seq_action(conn, 'stab_river', stat_cond, same_player=False, join_hands=True)
    
    # All potential flop donks (betting into pf agressor before aggressor can act)
    base_cond = (f'street = {FLOP_VAL} AND player_id != pf_agg_id'
                 + ' AND pf_agg_id IS NOT NULL AND bet_level = 0')
    base_cond2 = 'n_stab_flop IS NULL'
    calc_action(conn, 'n_donk_flop', base_cond, base_cond2, join_hands=True)
    
    # Donk flop (bet into preflop aggressor before aggressor can act)
    # p. 295
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    calc_action(conn, 'donk_flop', stat_cond, base_cond2, join_hands=True)

    # All potential turn donks (betting into flop agressor before aggressor can act)
    base_cond = (f'street = {TURN_VAL} AND player_id != flop_agg_id'
                 + ' AND flop_agg_id IS NOT NULL AND bet_level = 0')
    base_cond2 = 'n_stab_turn IS NULL'
    calc_action(conn, 'n_donk_turn', base_cond, base_cond2, join_hands=True)
    
    # Donk turn (bet into flop aggressor before aggressor can act)
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    calc_action(conn, 'donk_turn', stat_cond, base_cond2, join_hands=True)

    # All potential river donks (betting into turn agressor before aggressor can act)
    base_cond = (f'street = {RIVER_VAL} AND player_id != turn_agg_id'
                 + ' AND turn_agg_id IS NOT NULL AND bet_level = 0')
    base_cond2 = 'n_stab_river IS NULL'
    calc_action(conn, 'n_donk_river', base_cond, base_cond2, join_hands=True)
    
    # Donk river (bet into turn aggressor before aggressor can act)
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    calc_action(conn, 'donk_river', stat_cond, base_cond2, join_hands=True)


def run_rfi_stats(conn):
    '''Helper method for calling each of the raise first in stats methods.'''
    
    # All potential raise first in from any position (opp to open preflop)
    # Excludes blind posts by limiting to action_id >= FOLD_VAL
    base_cond = f'street = {PREFLOP_VAL} AND bet_level = 0'
    base_cond2 = base_cond + f' AND action_id >= {FOLD_VAL}'
    calc_action(conn, 'n_rfi', base_cond2)
    
    # Raise first in from any position (open preflop with a raise)
    stat_cond = base_cond + f' AND action_id = {RAISE_VAL}'
    calc_action(conn, 'rfi', stat_cond)
    
    for pos_index in range(POS_MIN, POS_MAX + 1):
        
        # All potential raise first in from given position (opp to open preflop)
        base_cond2 = base_cond + f' AND pos = {pos_index}'
        stat_cond = base_cond2 + f' AND action_id >= {FOLD_VAL}'
        calc_action(conn, f'n_rfi_{pos_index}', stat_cond, join_p_hands=True)
        
        # Raise first in from given position (open preflop with a raise)
        # p. 177
        stat_cond = base_cond2 + f' AND action_id = {RAISE_VAL}'
        calc_action(conn, f'rfi_{pos_index}', stat_cond, join_p_hands=True)

    # All potential raise first in from BB (opp to open preflop)
    base_cond2 = base_cond + f' AND pos BETWEEN {BB_VAL} AND {SB_VAL}'
    stat_cond = base_cond2 + f' AND action_id >= {FOLD_VAL}'
    calc_action(conn, 'n_rfi_bb', stat_cond, join_p_hands=True)
    
    # Raise first in from BB (open preflop with a raise)
    stat_cond = base_cond2 + f' AND action_id = {RAISE_VAL}'
    calc_action(conn, 'rfi_bb', stat_cond, join_p_hands=True)

    # All potential raise first in from SB (opp to open preflop)
    base_cond2 = base_cond + f' AND pos > {SB_VAL}'
    stat_cond = base_cond2 + f' AND action_id >= {FOLD_VAL}'
    calc_action(conn, 'n_rfi_sb', stat_cond, join_p_hands=True)
    
    # Raise first in from SB (open preflop with a raise)
    stat_cond = base_cond2 + f' AND action_id = {RAISE_VAL}'
    calc_action(conn, 'rfi_sb', stat_cond, join_p_hands=True)


def run_win_stats(conn):
    '''Helper method for calling each of the winning stats methods.'''
    
    # All potential won when seeing flop (took action on flop)
    # needs to check if player was active on flop, not took action (player could be all-in preflop) XXX
    base_cond = f'street = {FLOP_VAL}'
    calc_action(conn, 'n_wwsf', base_cond)
    
    # Won when seeing flop
    # p. 225
    stat_cond = base_cond + ' AND balance > 0'
    calc_action(conn, 'wwsf', stat_cond, join_p_hands=True)
    
    # All went to showdown
    # Already in stats as saw_sd
    # p. 137
    '''What is the denominator for went to showdown %? All hands? VPIP hands?
    Using hands that saw the flop (n_wwsf) in Tableau.'''
    
    # Won at showdown (went to showdown and won with balance > 0)
    # Should this count a pure chop (balance == 0)? XXX
    # p. 137
    stat_cond = 'saw_sd = 1 AND balance > 0'
    calc_action(conn, 'won_sd', stat_cond, join_p_hands=True)
    
    # All potential showed cards with win (won without showdown)
    base_cond = 'saw_sd = 0 AND balance > 0'
    calc_action(conn, 'n_show', base_cond, join_p_hands=True)
    
    # Showed cards with win (won without showdown and cards are known)
    stat_cond = base_cond + ' AND card1 IS NOT NULL AND card2 IS NOT NULL'
    calc_action(conn, 'show', stat_cond, join_p_hands=True)
    
    # stack size in terms of BB
    subq = 'SELECT * FROM PlayerHands JOIN Hands USING (table_id, hand_num)'
    set_value(conn, 'stack_bb', 'other.stack / (other.bb_amt * 1.0)', subq)

    # balance in terms of BB
    set_value(conn, 'balance_bb', 'other.balance / (other.bb_amt * 1.0)', subq)


def run_agg_stats(conn):
    '''Helper method for calling each of the aggression stats methods.'''
    
    # All potential preflop limp-raises (opp to call then raise preflop)
    base_cond = (f'street = {PREFLOP_VAL} and a1.action_id = {CALL_VAL}'
                 + ' AND a1.bet_level <= 1')
    stat_cond = base_cond + f' AND a2.action_id >= {FOLD_VAL}'
    calc_seq_action(conn, 'n_limpr', stat_cond)

    # Preflop limp-raise (call then raise preflop)
    stat_cond = base_cond + f' AND a2.action_id = {RAISE_VAL}'
    calc_seq_action(conn, 'limpr', stat_cond)

    # All potential flop check-raises
    street_cond = f'street = {FLOP_VAL}'
    base_cond = f' AND a1.action_id = {CHECK_VAL}'
    base_cond2 = f' AND a2.action_id >= {FOLD_VAL}'
    stat_cond = street_cond + base_cond + base_cond2
    calc_seq_action(conn, 'n_cr_flop', stat_cond)
    
    # Flop check-raise
    base_cond3 = f' AND a2.action_id = {RAISE_VAL}'
    stat_cond = street_cond + base_cond + base_cond3
    calc_seq_action(conn, 'cr_flop', stat_cond)
    
    # All potential turn check-raises
    street_cond = f'street = {TURN_VAL}'
    stat_cond = street_cond + base_cond + base_cond2
    calc_seq_action(conn, 'n_cr_turn', stat_cond)
    
    # Turn check-raise
    stat_cond = street_cond + base_cond + base_cond3
    calc_seq_action(conn, 'cr_turn', stat_cond)

    # All potential river check-raises
    street_cond = f'street = {RIVER_VAL}'
    stat_cond = street_cond + base_cond + base_cond2
    calc_seq_action(conn, 'n_cr_river', stat_cond)
    
    # River check-raise
    stat_cond = street_cond + base_cond + base_cond3
    calc_seq_action(conn, 'cr_river', stat_cond)
    
    # All potential flop check-raise cbets (opp to check then raise as pfr)
    # Alternatively, n_cbet_flop = 1 and n_cr_flop = 1
    base_cond = (f'street = {FLOP_VAL} AND a1.bet_level = 0'
                 + f' AND a1.player_id = pf_agg_id AND a1.action_id = {CHECK_VAL}')
    stat_cond = base_cond + f' AND a2.action_id >= {FOLD_VAL}'
    calc_seq_action(conn, 'n_cr_cbet_flop', stat_cond, join_hands=True)
    
    # Flop check-raise cbets (check then raise as pfr)
    # Alternatively, n_cbet_flop = 1 and cr_flop = 1
    stat_cond = base_cond + f' AND a2.action_id = {RAISE_VAL}'
    calc_seq_action(conn, 'cr_cbet_flop', stat_cond, join_hands=True)
    
    # All potential check-raise cbets on any street
    base_cond = ('(n_cbet_flop = 1 AND n_cr_flop = 1)'
                 + ' OR (n_cbet_turn = 1 AND n_cr_turn = 1)'
                 + ' OR (n_cbet_river = 1 AND n_cr_river = 1)')
    calc_action(conn, 'n_cr_cbet_any', 'True', base_cond)
    
    # Check-raise cbet on any street
    base_cond = ('(n_cbet_flop = 1 AND cr_flop = 1)'
                 + ' OR (n_cbet_turn = 1 AND cr_turn = 1)'
                 + ' OR (n_cbet_river = 1 AND cr_river = 1)')
    calc_action(conn, 'cr_cbet_any', 'True', base_cond)


def run_counting_stats(conn):
    '''Helper method for calling each of the counting stats methods.'''

    # All postflop actions
    base_cond = f'street >= {FLOP_VAL}'
    stat_cond = base_cond + f' AND action_id >= {FOLD_VAL}'
    count_action(conn, 'n_actions', stat_cond)

    # Postflop folds
    stat_cond = base_cond + f' AND action_id = {FOLD_VAL}'
    count_action(conn, 'n_folds', stat_cond)

    # Postflop checks
    stat_cond = base_cond + f' AND action_id = {CHECK_VAL}'
    count_action(conn, 'n_checks', stat_cond)

    # Postflop calls
    stat_cond = base_cond + f' AND action_id = {CALL_VAL}'
    count_action(conn, 'n_calls', stat_cond)
    
    # Postflop bets
    stat_cond = base_cond + f' AND action_id = {BET_VAL}'
    count_action(conn, 'n_bets', stat_cond)
    
    # Postflop raises
    stat_cond = base_cond + f' AND action_id = {RAISE_VAL}'
    count_action(conn, 'n_raises', stat_cond)
    
    # Postflop check-raises. Counting stat for convenience;
    # should generally use dedicated check-raise stats.
    # Number not subtracted from raw check or raise totals.
    stat_cond = base_cond + (f' AND prev_act_id = {CHECK_VAL}'
                             + f' AND action_id = {RAISE_VAL}')
    count_action(conn, 'n_check_rs', stat_cond)
    

def run_pvp_stats(conn):
    '''Calculates PvP nemesis-hero stats.'''
    
    print('PvP', end=' ')
    
    cur = conn.cursor()
    
    query = '''SELECT table_id, hand_num, player_id, balance, bb_amt,
        (SELECT SUM(balance)
            FROM PlayerHands AS ph2
            WHERE ph1.table_id = ph2.table_id AND ph1.hand_num = ph2.hand_num
            AND balance > 0
            GROUP BY table_id, hand_num) AS balance_sum
        FROM PlayerHands AS ph1 JOIN Hands USING (table_id, hand_num)
        WHERE (table_id, hand_num) IN NewHands
        AND balance > 0
        ORDER BY table_id, hand_num, player_id'''
        
    winner_hands = cur.execute(query).fetchall()
    
    for winner_hand in winner_hands:
        # print("Winner:", winner_hand)
        (table_id, hand_num, winner_id, winner_balance,
         bb_amt, balance_sum) = winner_hand

        query = '''SELECT player_id, balance
            FROM PlayerHands
            WHERE table_id = ? AND hand_num = ?
            AND balance < 0
            ORDER BY player_id'''
            
        values = (table_id, hand_num)
        loser_hands = cur.execute(query, values).fetchall()
        
        for loser_hand in loser_hands:
            # print("...loser:", loser_hand)
            (loser_id, loser_balance) = loser_hand
            # hacky way to distribute losses when there are multiple winners
            loser_balance = loser_balance * (winner_balance / balance_sum)
            
            # set winner as gaining -loser_balance from loser
            query = '''INSERT INTO StatPvP
                (table_id, hand_num, player_id, opp_id, net_chips, net_bb)
                VALUES (?, ?, ?, ?, ?, ?)'''
            values = (table_id, hand_num, winner_id, loser_id,
                      -loser_balance, -loser_balance / bb_amt)
            cur.execute(query, values)
            
            # set loser as losing loser_balance to winner
            query = '''INSERT INTO StatPvP
                (table_id, hand_num, player_id, opp_id, net_chips, net_bb)
                VALUES (?, ?, ?, ?, ?, ?)'''
            values = (table_id, hand_num, loser_id, winner_id,
                      loser_balance, loser_balance / bb_amt)
            cur.execute(query, values)
    
    conn.commit()
    cur.close()


def run_final_stats(conn):
    '''Helper method for calling each of the final stats methods.'''
        
    cur = conn.cursor()
    
    # Set row_num in order
    # used to use DESNE_RANK() instead of ROW_NUMBER() for some reason
    subq = '''SELECT ROW_NUMBER() OVER (ORDER BY time, table_id, hand_num) rn,
        table_id, hand_num, player_id
        FROM StatPlayerHands JOIN Hands USING (table_id, hand_num)
        ORDER BY time'''
    set_value(conn, 'row_num', 'other.rn', subq, null_only=False)

    # Calculate running bb balances
    # (Tableau calculates these natively, but Power BI is a disaster)
    """
    subq = '''SELECT player_id, row_num, balance_bb,
        SUM(balance_bb) OVER (PARTITION BY player_id ORDER BY row_num) AS running_total
        FROM StatPlayerHands
        ORDER BY player_id'''
    set_value(conn, 'bal_bb_running', 'other.running_total', subq,
              match_row=True, null_only=False)
    """
    
    # Set sess_num in order
    print('sess_num', end=' ')
    end_time = '2099-12-31'
    recent_cur = split_sessions(conn, return_all=True)
    for row in recent_cur:
        # print(row)
        begin_time = row[0]
        sess_num = row[5]
        """
        query = '''UPDATE StatPlayerHands
                SET sess_num = ?
                WHERE table_id IN
                (SELECT table_id
                FROM TableNames
                WHERE time >= ? AND time < ?)'''
        """
        query = '''UPDATE TableNames
                        SET sess_num = ?
                        WHERE time >= ? AND time < ?'''
        values = (sess_num, begin_time, end_time)
        cur.execute(query, values)
        end_time = begin_time
    
    recent_cur.close()

    # Above query doesn't work for first session in db
    query = 'UPDATE TableNames SET sess_num = 1 WHERE sess_num IS NULL'
    # query = 'UPDATE TableNames SET sess_num = 1 WHERE time = (SELECT MIN(time) FROM TableNames)'
    cur.execute(query)
    
    # subq = split_sessions(None, return_query=True)
    # set_value(conn, 'sess_num', 'other.sn', subq, values=(TIME_DIFF,),
    #           match_player=False, null_only=False)
        
    # Set timestamp
    calc_action(conn, 'date_added', 'True', val='CURRENT_TIMESTAMP')
    
    conn.commit()
    cur.close()


def run_tourney_stats(conn):
    '''Helper method for calling tournament stats.'''

    print()
    print('Calculating tournament stats...')

    cur = conn.cursor()

    # add sess_num to TourneyActions
    # can't rely on JOIN with TableNames because later DELETE won't work
    query = '''UPDATE TourneyActions
    SET sess_num = (
      SELECT tn.sess_num
      FROM TableNames AS tn
      WHERE TourneyActions.table_id = tn.table_id
    );'''
    cur.execute(query)
    conn.commit()

    # add prev_action_id to TourneyActions
    # also not strictly necessary, but makes queries easier
    query = '''UPDATE TourneyActions AS ta
    SET prev_action_id = 
        (SELECT ta2.t_action_id
        FROM TourneyActions AS ta2
        WHERE ta.sess_num = ta2.sess_num
          AND ta.player_id = ta2.player_id
          AND ta.time > ta2.time
        ORDER BY ta2.time DESC
        LIMIT 1)'''
    cur.execute(query)
    conn.commit()

    # delete rows from TourneyActions where a player enters the game but did not previously quit
    # necessary to deal with the hand history ambiguity
    # need to manually fix hand histories where a player changed names but the timestamp for the old name quitting
    #  is later than the timestamp of the new name joining
    query = '''DELETE FROM TourneyActions
        WHERE t_action_id = ? AND prev_action_id > ?'''
    values = (BUYIN_VAL, QUIT_VAL)
    cur.execute(query, values)

    # find tournament winners
    query = '''SELECT sess_num, 1 as place, player_id
        FROM (
            SELECT ta.sess_num, ta.player_id, t_action_id AS recent_action, MAX(ta.time)
            FROM TourneyActions ta
            GROUP BY ta.sess_num, ta.player_id
        ) AS recent_action_subquery
        WHERE recent_action > ?'''
    values = (QUIT_VAL,)
    cur.execute(query, values)
    winner_list = cur.fetchall()

    # find other tournament finishers
    # could break ties by stack size XXX
    query = '''SELECT subq.sess_num,
        ROW_NUMBER() OVER (PARTITION BY subq.sess_num ORDER BY subq.time DESC) + 1 AS place,
        subq.player_id
        FROM (
          SELECT ta.*
          FROM TourneyActions ta
          WHERE ta.t_action_id = ?
            AND NOT EXISTS (
              SELECT 1
              FROM TourneyActions ta2
              WHERE ta2.t_action_id > ?
                AND ta2.player_id = ta.player_id
                AND ta2.sess_num = ta.sess_num
                AND ta2.time > ta.time
            )
        ) subq
        ORDER BY subq.sess_num, place'''
    values = (QUIT_VAL, QUIT_VAL)
    cur.execute(query, values)
    place_list = cur.fetchall()

    # insert tournament places into StatTourneyPlaces
    combined_list = winner_list + place_list
    combined_list.sort()  # sorting on (sess_num, place) keeps table more human readable

    query = '''INSERT INTO StatTourneyPlaces
        (sess_num, place, player_id)
        VALUES (?, ?, ?)'''

    for row in combined_list:
        # (sess_num, place, player_id) = row
        # print(row)
        # UNIQUE constraint can fail in tournament hands
        # sqlite3.IntegrityError: UNIQUE constraint failed: StatTourneyPlaces.sess_num, StatTourneyPlaces.player_id
        err = try_query(cur, query, row)
        if err == 'IntegrityError':
            print(f'...Error on sess_num/place/player_id: {row}')
        # cur.execute(query, row)

    # insert session hand number, total stacks, and total active players into StatTourneyHands
    # sess_num is again not strictly necessary but much more readable
    query = '''INSERT INTO StatTourneyHands (sess_num, sess_hand, table_id, hand_num, total_stacks, total_players)
        SELECT tn.sess_num,
        ROW_NUMBER() OVER (ORDER BY h.time, h.table_id, h.hand_num) AS sess_hand,
        h.table_id, h.hand_num,
        (
            SELECT SUM(ta.amount)
            FROM TourneyActions ta
            WHERE ta.sess_num = tn.sess_num AND ta.time <= h.time
        ) AS total_stacks,
        (
            SELECT COUNT(*)
            FROM (
                SELECT ta.sess_num, ta.player_id, t_action_id AS recent_action, MAX(ta.time)
                FROM TourneyActions ta
                WHERE ta.sess_num = tn.sess_num AND ta.time <= h.time
                GROUP BY ta.sess_num, ta.player_id
            ) AS recent_action_subquery
            WHERE recent_action > 0
        ) AS total_players
        FROM Hands h JOIN TableNames tn USING (table_id)
        ORDER BY h.time'''
    cur.execute(query)

    conn.commit()
    cur.close()


def run_small_db(source_db):
    '''Create the small (monthly) db.'''
    
    # copy to small db
    print(f'Copying {source_db} to {SMALL_DB_NAME}...')
    shutil.copyfile(source_db, SMALL_DB_NAME)
    
    # small_conn = sqlite3.connect(SMALL_DB_NAME)
    # context statement for the win!
    with sqlite3.connect(SMALL_DB_NAME) as small_conn:
        small_cur = small_conn.cursor()
    
        # find begin date within SMALL_DAYS (e.g. 31)
        begin_date = split_sessions(small_conn, num_days=SMALL_DAYS)
        # print(f'Begin date: {begin_date}')
        
        # remove all tables other than those needed by Tableau
        print('Removing unnecessary tables...')
        table_list = ['ActionNames', 'Actions', 'Aliases', 'Stats']
        for table_name in table_list:
            print(f'...Deleting {table_name}')
            query = f'DROP TABLE IF EXISTS {table_name}'
            small_cur.execute(query)
        
        # find db table names
        query = '''SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name'''
        small_cur.execute(query)
        table_list = small_cur.fetchall()
        
        # do TableNames db table last (otherwise query will fail!)
        try:
            table_list.remove(('TableNames',))
        except ValueError:
            print('No TableNames table found')
        table_list.append(('TableNames',))
        
        print(f'Deleting old hands before {begin_date}...')
        # print(table_list)
        
        # go through each db table and delete all data older than begin date
        for table_tuple in table_list:
            table_name = table_tuple[0]
            
            query = f'DELETE FROM {table_name} WHERE table_id IN'
            query += ''' (SELECT table_id FROM TableNames
                    WHERE time < ?
                    GROUP BY table_id)'''
            
            values = (begin_date,)
            try:
                small_cur.execute(query, values)
                print(f'...Removed old hands from {table_name}')
            except sqlite3.OperationalError:
                # no 'table_id' column found for this particular table
                print(f'...Skipped {table_name}')
            
            # end for block
        
        # replace player last names
        print('Replacing last names...')
        query = '''UPDATE PlayerNames
                SET last_name = SUBSTR(last_name, 1, 1)'''
        small_cur.execute(query)
        
        # remove owner hole cards
        print('Removing owner hole cards...')
        query = '''UPDATE PlayerHands
                SET own_c1 = NULL, own_c2 = NULL'''
        small_cur.execute(query)
        
        # end 'with' context statement
        
    print('Reducing file size...')
    small_conn = sqlite3.connect(SMALL_DB_NAME)
    small_conn.execute('VACUUM')
    small_conn.commit()
    

# main body
if __name__ == '__main__':

    if TEST_RUN:
        print('<<< TEST RUN >>>')

    # loop through each database
    main_db_actions = 0
    source_db_name = None
    for db in DB_LIST:

        real_db_name = db['db_name']
        test_db_name = db['db_name_test']
        is_tourney = db['is_tourney']
        clear_db = db['clear_db']
        if TEST_RUN:
            db_name = test_db_name
        else:
            db_name = real_db_name

        # connect to database
        with sqlite3.connect(db_name) as conn:
            # conn = sqlite3.connect(db_name)
            print()
            print(f'******************** Connecting to database {db_name} ********************')

            create_stats_table_sparse(conn, db_name, clear_db)
            num_new_actions = create_new_actions_table(conn)

            # if this is the main database (real or test run), store relevant info for copying to small database
            if db_name == DB_LIST[0]['db_name'] or db_name == DB_LIST[0]['db_name_test']:
                main_db_actions = num_new_actions
                source_db_name = db_name

            # skip if no new actions are added
            if num_new_actions > 0:
                create_new_hands_table(conn)

                # create_stats_table(conn)

                # call helper methods
                run_preflop_stats(conn)
                run_cbet_stats(conn)
                run_stab_stats(conn)
                run_rfi_stats(conn)
                run_win_stats(conn)
                run_agg_stats(conn)
                run_counting_stats(conn)
                run_pvp_stats(conn)
                run_final_stats(conn)

                # run tournament stats
                # should tournament stats be run even if no new actions are added? XXX
                if is_tourney:
                    run_tourney_stats(conn)

            print()
            print()

    # run outside main database connection so complete db is copied
    if main_db_actions > -1:
        # sleep_time = 3
        # print()
        # print(f'Pausing for {sleep_time} seconds...')
        # time.sleep(sleep_time)
        run_small_db(source_db_name)