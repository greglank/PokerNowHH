# -*- coding: utf-8 -*-
"""
Created on Sat Jul  9 16:47:54 2022

@author: gregl

Wish list (XXX):
-Toggle showing player hole cards
-Later street pot calculation when players are all-in
-Side pot winnings (table 577, hand 45)

Error list (XXX):
-Table: 553 (pglQJrIlk_L0h3HmRZUzZy7tB); Hand: 35
    •CoreyL checks, MikeA bets 37.50 bb (75% pot), WillJ raises 75.00 bb (30% pot), CoreyL folds,
    MikeA raises 38.80 bb (0% pot), WillJ calls all-in

"""

# import pandas as pd
import sqlite3  # sqlite database
from Hearts.playing_card import PlayingCard  # uses PlayingCard; could instead use JOIN commands to CardNames
from datetime import datetime  # for converting time strings

test_run = False
show_cards = False  # show hole cards every time player acts

# streets for internal representation and for database
PREFLOP_VAL = 1
FLOP_VAL = 3  # flop=3 is more human readable
TURN_VAL = 4
RIVER_VAL = 5
SHOWDOWN_VAL = 6

# blind values to add to position; SB is bigger because it's the worst position
STRADDLE_VAL = 10
BB_VAL = 20
SB_VAL = 30
POS_NAMES = ['None', 'BTN', 'CO', 'HJ', 'UTG6', 'UTG7', 'UTG8', 'UTG9', 'UTG10']

# OWNER_ID = 2  # player_id for showing table owner's cards
TIME_DIFF = 0.2  # time difference (in days) between session hands
BOARD_CARDS = 5  # number of cards on the board (5)

# date and time
TIME_FORMAT_PARSER = '%Y-%m-%d %H:%M'

def print_board(street, board, board2, pot, eff_stack, bb_amt, num_players, old_board, new_board, use_bb):
    '''Prints the board cards and pot.
    Returns old_board and new_board strings to help with calling the
    method again on the next street.'''
    
    if street == FLOP_VAL:
        street_name = 'Flop'
        new_board = f'{board[0]}, {board[1]}, {board[2]}'
    elif street == TURN_VAL:
        street_name = 'Turn'
        old_board = f'{new_board}, '
        new_board = f'{board[3]}'
    elif street == RIVER_VAL:
        street_name = 'River'
        old_board = f'{old_board}{new_board}, '
        new_board = f'{board[4]}'
    elif street == SHOWDOWN_VAL:
        # run it twice board
        street_name = 'SECOND BOARD'
        old_board = ''
        new_board = ''
        # find where run it twice cards differ
        for card_index in range(0, BOARD_CARDS):
            if str(board[card_index]) == str(board2[card_index]):
                old_board += f'{board[card_index]}, '
            else:
                new_board += f'{board2[card_index]}, '
        # chop off last comma and space (super hacky)
        if len(new_board) >= 2:
            new_board = new_board[:-2]
    else:
        street_name = 'Preflop'
        
    if use_bb:
        amount = f'pot {(pot / bb_amt):.2f} bb'
        if eff_stack[street]:
            amount += f'; eff {(eff_stack[street] / bb_amt):.2f} bb'
    else:
        amount = f'pot ${(pot / 100):.2f}'
        if eff_stack[street]:
            amount += f'; eff ${(eff_stack[street] / 100):.2f}'
        
    print()
    print()
    print(f'{street_name}', end='')
    if street >= FLOP_VAL:
        print(f' ({amount})', end='')
    print(f': ', end='')
    if street == PREFLOP_VAL:
        print(f'{num_players} Players', end='')
    if street >= FLOP_VAL:
        print(f'{old_board}[{new_board}]', end='')
        # for i in range(0, street):
        #    print(f' {board[i]}', end='')
    return (old_board, new_board)


def get_hand_info(cur, target_table, target_hand):

    # get Hand info
    """
    query = '''SELECT Hands.time, table_id, table_name, hand_num, num_players, bb_amt,
        c1.card_sym AS flop1, c2.card_sym AS flop2, c3.card_sym AS flop3,
        c4.card_sym AS turn, c5.card_sym AS river
        FROM Hands JOIN TableNames USING (table_id)
        LEFT JOIN CardNames AS c1 ON flop_card1 = c1.card_id
        LEFT JOIN CardNames AS c2 ON flop_card2 = c2.card_id
        LEFT JOIN CardNames AS c3 ON flop_card3 = c3.card_id
        LEFT JOIN CardNames AS c4 ON turn_card = c4.card_id
        LEFT JOIN CardNames AS c5 ON river_card = c5.card_id
        WHERE table_id = ? AND hand_num = ?
        ORDER BY table_id, hand_num'''
    """

    query = '''SELECT Hands.time, table_id, table_name, hand_num, num_players, bb_amt,
            fc1_val, fc2_val, fc3_val, tc_val, rc_val,
            fc1_suit, fc2_suit, fc3_suit, tc_suit, rc_suit,
            f2c1_val, f2c2_val, f2c3_val, t2c_val, r2c_val,
            f2c1_suit, f2c2_suit, f2c3_suit, t2c_suit, r2c_suit,
            eff_pf, eff_flop, eff_turn, eff_river
            FROM Hands JOIN TableNames USING (table_id)
            WHERE table_id = ? AND hand_num = ?
            ORDER BY table_id, hand_num'''

    values = (target_table, target_hand)
    cur.execute(query, values)
    
    # print Hand info
    board = [None] * BOARD_CARDS
    val = [None] * BOARD_CARDS
    suit = [None] * BOARD_CARDS
    board2 = [None] * BOARD_CARDS
    val2 = [None] * BOARD_CARDS
    suit2 = [None] * BOARD_CARDS
    eff_stack = [0] * (SHOWDOWN_VAL + 1)
    """
    (time, table_id, table_name, hand_num, num_players, bb_amt,
     board[0], board[1], board[2], board[3], board[4]) = cur.fetchone()
    """
    (time, table_id, table_name, hand_num, num_players, bb_amt,
     val[0], val[1], val[2], val[3], val[4],
     suit[0], suit[1], suit[2], suit[3], suit[4],
     val2[0], val2[1], val2[2], val2[3], val2[4],
     suit2[0], suit2[1], suit2[2], suit2[3], suit2[4],
     eff_stack[PREFLOP_VAL], eff_stack[FLOP_VAL], eff_stack[TURN_VAL], eff_stack[RIVER_VAL]) = cur.fetchone()

    # convert to PlayingCard (watch out for downstream errors; board[] used to be strings XXX)
    for idx in range(0, BOARD_CARDS):
        # board[idx] = PlayingCard.convert_value(val[idx]) + PlayingCard.convert_suit(suit[idx])
        if val[idx] is not None and suit[idx] is not None: board[idx] = PlayingCard(val[idx], suit[idx])
        if val2[idx] is not None and suit2[idx] is not None: board2[idx] = PlayingCard(val2[idx], suit2[idx])

    dt = datetime.fromisoformat(time)

    for idx in range(len(eff_stack)):
        if eff_stack[idx] is None:
            eff_stack[idx] = 0

    print()
    print(f'Table: {table_id} ({table_name});'
          + f' Hand: {hand_num};'
          + f' bb: ${(bb_amt / 100):.2f};'
          + f' Date: {dt.strftime(TIME_FORMAT_PARSER)}')
    
    return board, board2, eff_stack, bb_amt, num_players


def get_playerhand_info(cur, target_table, target_hand, bb_amt, use_bb):

    # get PlayerHand info
    
    query = '''SELECT first_name || substr(last_name,1,1) AS full_name, stack, pos
        FROM PlayerHands JOIN PlayerNames USING (player_id)
        WHERE table_id = ? AND hand_num = ?
        ORDER BY table_id, hand_num, pos DESC'''
    values = (target_table, target_hand)
    cur.execute(query, values)
    
    # print PlayerHand info
       
    print('Stacks: ', end='')
    for row in cur:
        (full_name, stack, pos) = row
        if use_bb:
            amount = f'{(stack / bb_amt):.2f} bb'
        else:
            amount = f'${(stack / 100):.2f}'
        print(f'{full_name} {amount}', end='')
        if pos != 1:
            print(', ', end='')


def get_actions_info(cur, target_table, target_hand, board, board2, eff_stack, bb_amt, num_players,
                     owner_id, show_self, advanced, req, use_bb):

    # get Actions info
    """
    query = '''SELECT street, action_num, player_id,
        first_name || substr(last_name,1,1) AS full_name,
        action_name, amount, net_amount, allin_flag, pot, to_call,
        pos, pc1.card_sym AS pcard1, pc2.card_sym AS pcard2,
        oc1.card_sym AS ocard1, oc2.card_sym AS ocard2
        FROM Actions JOIN PlayerNames USING (player_id)
        JOIN ActionNames USING (action_id)
        JOIN PlayerHands USING (table_id, hand_num, player_id)
        LEFT JOIN CardNames AS pc1 ON card1 = pc1.card_id
        LEFT JOIN CardNames AS pc2 ON card2 = pc2.card_id
        LEFT JOIN CardNames AS oc1 ON own_c1 = oc1.card_id
        LEFT JOIN CardNames AS oc2 ON own_c2 = oc2.card_id
        WHERE table_id = ? AND hand_num = ?
        ORDER BY table_id, hand_num, action_num'''
    """
    query = '''SELECT street, action_num, player_id,
            first_name || substr(last_name,1,1) AS full_name,
            action_name, amount, net_amount, allin_flag, pot, to_call, pos,
            card1_val, card2_val, card1_suit, card2_suit,
            own_c1_val, own_c2_val, own_c1_suit, own_c2_suit
            FROM Actions JOIN PlayerNames USING (player_id)
            JOIN ActionNames USING (action_id)
            JOIN PlayerHands USING (table_id, hand_num, player_id)
            WHERE table_id = ? AND hand_num = ?
            ORDER BY table_id, hand_num, action_num'''
    values = (target_table, target_hand)
    cur.execute(query, values)

    # col_names = [desc[0] for desc in cur.description]
    # print(col_names)
    
    # print Actions info
    
    old_board = ''
    new_board = ''
    old_street = 0
    old_pos = 99
    old_action = None
    board_info = (old_board, new_board, old_street, old_pos, old_action)
    
    for row in cur:
        board_info = print_actions_info(row, board_info, board, board2, eff_stack, bb_amt, num_players,
                                        owner_id, show_self, advanced, req, use_bb)
        
    # if players are all in before river, continue printing board cards
    (old_board, new_board, old_street, old_pos, old_action) = board_info
    """(street, action_num, player_id, full_name, action_name,
     amount, net_amount, allin_flag, pot, to_call,
     pos, pcard1, pcard2, ocard1, ocard2) = row"""

    (street, action_num, player_id, full_name, action_name,
     amount, net_amount, allin_flag, pot, to_call, pos,
     card1_val, card2_val, card1_suit, card2_suit,
     own_c1_val, own_c2_val, own_c1_suit, own_c2_suit) = row

    if street < RIVER_VAL and board[4] is not None:
        for idx_street in range(max(street + 1, FLOP_VAL), RIVER_VAL + 1):
            (old_board, new_board) = print_board(idx_street, board, board2, pot, eff_stack, bb_amt, num_players,
                                                 old_board, new_board, use_bb)

    if board2[4] is not None:
        (old_board, new_board) = print_board(SHOWDOWN_VAL, board, board2, pot, eff_stack, bb_amt, num_players,
                                             old_board, new_board, use_bb)


def print_actions_info(row, board_info, board, board2, eff_stack, bb_amt, num_players,
                       owner_id, show_self, advanced, req, use_bb, new_line=False):

    # print(row)
    """(street, action_num, player_id, full_name, action_name,
     amount, net_amount, allin_flag, pot, to_call,
     pos, pcard1, pcard2, ocard1, ocard2) = row"""
    (street, action_num, player_id, full_name, action_name,
     amount, net_amount, allin_flag, pot, to_call, pos,
     card1_val, card2_val, card1_suit, card2_suit,
     own_c1_val, own_c2_val, own_c1_suit, own_c2_suit) = row

    pcard1 = pcard2 = ocard1 = ocard2 = None
    if card1_val is not None and card2_val is not None:
        pcard1 = PlayingCard(card1_val, card1_suit)
        pcard2 = PlayingCard(card2_val, card2_suit)
    if own_c1_val is not None and own_c2_val is not None:
        ocard1 = PlayingCard(own_c1_val, own_c1_suit)
        ocard2 = PlayingCard(own_c2_val, own_c2_suit)

    (old_board, new_board, old_street, old_pos, old_action) = board_info

    if show_cards or street == PREFLOP_VAL:
        if (show_self and player_id == owner_id
           and ocard1 is not None and ocard2 is not None):
            pcards = f' [{ocard1}, {ocard2}]'
        elif pcard1 is not None and pcard2 is not None:
            pcards = f' [{pcard1}, {pcard2}]'
        else:
            pcards = ''
    else:
        pcards = ''

    if street == PREFLOP_VAL:
        if pos >= SB_VAL:
            pos_name = 'SB'
        elif pos >= BB_VAL:
            pos_name = 'BB'
        elif pos >= STRADDLE_VAL:
            pos_name = 'Straddle'
        else:
            pos_name = POS_NAMES[pos]
        pos_name = f'/{pos_name}'
    else:
        pos_name = ''
       
    # amount in bb or $
    if use_bb:
        amount = f'{(net_amount / bb_amt):.2f} bb'
    else:
        amount = f'${(net_amount / 100):.2f}'
    
    # bet/raise as percentage of pot (whole number percentage)
    if pot > 0:
        if action_name.lower() in ['raises']:
            # https://blog.gtowizard.com/how-to-calculate-raises-in-poker/
            # raise_amount = raise_% * (2 * bet + pot_before_bet) + bet
            # raise_% = (raise_amount - bet) / (2 * bet + pot_before_bet)
            bet_size = (net_amount - to_call) / (to_call + pot) * 100
        else:
            bet_size = net_amount / pot * 100
    else:
        bet_size = 0
    
    # required equity to call (whole number percentage)
    if (pot + to_call) > 0:
        r_equity = to_call / (pot + to_call) * 100
    else:
        r_equity = 0
        
    if street == old_street:
        if old_action not in ['posts', None]: print(', ', end='')
    else:
        (old_board, new_board) = print_board(street, board, board2, pot, eff_stack, bb_amt, num_players,
                                             old_board, new_board, use_bb)
        print()
        print('•', end='')
        old_street = street
        old_pos = 99

    # new line if player has taken multiple actions on this street
    if new_line:
        if street == PREFLOP_VAL:
            # preflop, reset after passing blinds
            # (or after passing button if blinds are out)
            if ((old_pos >= STRADDLE_VAL and pos < STRADDLE_VAL)
               or (old_pos < pos and pos < STRADDLE_VAL)):
                print()
                print('•', end='')
        else:
            # postflop, reset after passing smallest position (closest to button)
            if old_pos < pos:
                print()
                print('•', end='')
    old_pos = pos
    old_action = action_name.lower()

    # player name and action
    if action_name.lower() not in ['posts']:
        print(f'{full_name}{pos_name}{pcards} {action_name.lower()}', end='')

        # action amount
        if action_name.lower() in ['bets', 'raises']:
            print(f' {amount}', end='')

        # all-in
        if allin_flag:
            print(' all-in', end='')

        # required equity
        if req and (action_name.lower() in ['calls']
                         or (action_name.lower() in ['folds'] and street >= FLOP_VAL)):
            print(f' ({int(r_equity)}% req)', end='')

        # percentage of pot
        if advanced and action_name.lower() in ['bets', 'raises'] and street >= FLOP_VAL:
            print(f' ({int(bet_size)}% pot)', end='')
    
    return (old_board, new_board, old_street, old_pos, old_action)


def get_winner_info(cur, target_table, target_hand, bb_amt, use_bb):

    # get winner info
    """
    query = '''SELECT first_name || substr(last_name,1,1) AS full_name, balance,
        pc1.card_sym AS pcard1, pc2.card_sym AS pcard2
        FROM PlayerHands JOIN PlayerNames USING (player_id)
        LEFT JOIN CardNames AS pc1 ON card1 = pc1.card_id
        LEFT JOIN CardNames AS pc2 ON card2 = pc2.card_id
        WHERE table_id = ? AND hand_num = ?
        ORDER BY table_id, hand_num, balance'''
    """
    # LEFT JOIN PlayerStreets/ClassNames because class_id can be NULL
    # subquery selects max street where player's class_id is known
    # GROUP BY clause is necessary to remove duplicates from (class_id is NULL) condition
    query = '''SELECT first_name || substr(last_name,1,1) AS full_name, balance,
            card1_val, card2_val, card1_suit, card2_suit, saw_sd, class_name
            FROM PlayerHands JOIN PlayerNames USING (player_id)
            LEFT JOIN PlayerStreets ps USING (table_id, hand_num, player_id)
            LEFT JOIN ClassNames USING (class_id)
            WHERE table_id = ? AND hand_num = ?
            AND (class_id IS NULL OR street =
	            (SELECT MAX(street)
	            FROM PlayerStreets
	            WHERE table_id = ps.table_id AND hand_num = ps.hand_num AND player_id = ps.player_id)
	            )
            GROUP BY full_name
            ORDER BY table_id, hand_num, balance
            '''
    values = (target_table, target_hand)
    cur.execute(query, values)

    # amount in bb or $
    
    # print winner info
    
    print()
    print()
    for row in cur:
        # (full_name, balance, pcard1, pcard2) = row
        (full_name, balance, card1_val, card2_val, card1_suit, card2_suit, saw_sd, class_name) = row
        pcard1 = pcard2 = None
        if card1_val is not None and card1_suit is not None: pcard1 = PlayingCard(card1_val, card1_suit)
        if card2_val is not None and card2_suit is not None: pcard2 = PlayingCard(card2_val, card2_suit)
        if pcard1 is not None and pcard2 is not None:
            pcards = f' [{pcard1}, {pcard2}]'
        else:
            pcards = ''

        if use_bb:
            amount = f'{(balance / bb_amt):.2f} bb'
        else:
            amount = f'${(balance / 100):.2f}'

        if balance > 0:
            if class_name is not None:
                winning_rank = f' with a {class_name}'
            else:
                winning_rank = ''
            if saw_sd == 0:
                showdown = ' (no showdown)'
            else:
                showdown = ''
            print(f'{full_name}{pcards} wins {amount}{winning_rank}{showdown}')


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


def find_recent(conn, num_hands=10, num_sessions=1, num_days=0, owner_id=0, opp_id=0,
                show_self=True, known_only=False, advanced=True, req=False, use_bb=True):
    '''Finds the top (num_hands) hands in terms of pot size
    from most recent (num_sessions) session(s).
    Calls process_hand for each.
    Use num_sessions=0 for all sessions XXX.'''
    
    # to find (num_sessions) most recent sessions,
    # could also reverse sort query with limit = (num_sessions)
    
    begin_time = split_sessions(conn, num_days=num_days, num_sessions=num_sessions)
    
    # query that returns the top (num_hands) from the most recent session(s)
    # self hands don't work with test database (not always player 2 XXX)
    
    if owner_id > 0:
        join_hands = 'JOIN PlayerHands ph USING (table_id, hand_num)'
        my_hands = f'AND ph.player_id = {owner_id}'
        if opp_id > 0:
            join_hands += ' JOIN PlayerStreets ps1 USING (table_id, hand_num)' \
                          ' JOIN PlayerStreets ps2 USING (table_id, hand_num)'
            my_hands += f' AND (pf_agg_id = {owner_id} OR pf_agg_id = {opp_id})' \
                        f' AND ps1.player_id = {owner_id} AND ps1.street = {FLOP_VAL} AND ps1.is_active = 1' \
                        f' AND ps2.player_id = {opp_id} AND ps2.street = {FLOP_VAL} AND ps2.is_active = 1'
        order_by = 'ABS(ph.balance/bb_amt) DESC'
    else:
        join_hands = ''
        my_hands = ''
        order_by = '(win_amount/bb_amt) DESC'

    if owner_id > 0 and known_only:
        known_cards = ' AND card1 IS NOT NULL AND card2 IS NOT NULL'
    else:
        known_cards = ''
    
    query = f'''SELECT table_id, hand_num
        FROM Hands
        {join_hands}
        WHERE datetime(time) >= datetime(?)
        {my_hands}
        {known_cards}
        ORDER BY {order_by}
        LIMIT ?'''
    
    recent_cur = conn.cursor()
    # print(query)
    values = (begin_time, num_hands)
    recent_cur.execute(query, values)
    
    idx = 1
    for row in recent_cur:
        print()
        if owner_id > 0:
            if opp_id > 0:
                print(f'********* Largest Player {owner_id} vs. Player {opp_id} Amount #{idx} **********')
            else:
                print(f'********* Largest Player {owner_id} Amount #{idx} **********')
        else:
            print(f'********* Largest Table Amount #{idx} **********')
        idx += 1
        (target_table, target_hand) = row
        process_hand(cur, target_table, target_hand,
                     owner_id, show_self, advanced, req, use_bb)
        
    recent_cur.close()
    

def process_hand(cur, target_table, target_hand, owner_id=0,
                 show_self=True, advanced=True, req=False, use_bb=True):
    '''Helper method; calls other methods in sequence'''
    (board, board2, eff_stack, bb_amt, num_players) = get_hand_info(cur, target_table, target_hand)
    get_playerhand_info(cur, target_table, target_hand, bb_amt, use_bb)
    get_actions_info(cur, target_table, target_hand, board, board2, eff_stack, bb_amt, num_players,
                     owner_id, show_self, advanced, req, use_bb)
    get_winner_info(cur, target_table, target_hand, bb_amt, use_bb)


# main body
if __name__ == '__main__':
    # code to be executed only on direct execution, but not on import

    # moved from global variables section to here for easy editing
    if test_run:
        DATABASE_NAME = 'hand_history_test.sqlite'
    else:
        # DATABASE_NAME = 'hh_shakeweights.sqlite'
        DATABASE_NAME = 'hand_history.sqlite'

    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor()
    
    if test_run:
        print('TEST RUN')
    
    '''
    while target_table is None:
        target_table = input('Table: ')
    
    while target_hand is None:
        target_hand = input('Hand: ')
    
    '''
    
    # show top hands (player)
    find_recent(conn, num_hands=20, num_sessions=1, owner_id=2, opp_id=0,
                show_self=True, known_only=False, advanced=True, req=False, use_bb=True)

    # show top hands (all)
    find_recent(conn, num_hands=20, num_sessions=1, owner_id=0, opp_id=0,
                show_self=False, known_only=False, advanced=True, req=False, use_bb=True)
    print()

    # show selected hand
    # change targets to None to be prompted
    target_table = 262
    target_hand = 503
    print()
    print(f'********** Table {target_table}, Hand {target_hand} **********')
    process_hand(cur, target_table, target_hand, owner_id=0,
                 show_self=False, advanced=True, req=False, use_bb=True)
    
    # with sqlite3.connect(DATABASE_NAME) as conn:
    #    df = pd.read_sql(query, conn, params=values)
    
    # print(df.head())
    
    '''
    
    target_table = 1
    for target_hand in range(221, 242):
        print()
        print(f'********** Table {target_table}, Hand {target_hand} **********')
        process_hand(cur, target_table, target_hand,
                     owner_id=6, show_self=True, advanced=False, use_bb=True)
    
    '''
    
    cur.close()
    conn.close()
