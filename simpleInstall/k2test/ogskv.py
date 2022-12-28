#!/usr/bin/env python3

from curtsies import FullscreenWindow, Input, FSArray
from curtsies.fmtfuncs import red, bold, underline, green, on_blue, blue, black
import argparse
from skvclient import SKVClient
import ogskvquery


class ScreenData:
    def __init__(self, data, header, footer="(q) Exit, (Esc) back screen, (f) full record, (i) indexes"):
        self.selected = 0
        self.startidx = 0
        self.data = data
        self.header = header
        self.select_func = None
        self.raw_data = None
        self.footer = underline(bold(blue(footer)))

def get_header(records):
    if len(records) == 0:
        return underline(bold(blue("No data")))
    header = ""
    for k in records[0].data.keys():
        header += k + ", "
    return underline(bold(blue(header[:len(header)-2])))

def get_datalines(records):
    data = []
    for record in records:
        val = ""
        for v in record.data.values():
            val += str(v) + ", "
        data.append(val[:len(val)-2])
    return data

def refresh_screen(window, screen_data):
    screen = FSArray(window.height, window.width)
    width = min(screen_data.header.width, window.width)
    header = screen_data.header[:width]
    screen[0:1, 0:width] = [underline(bold(blue(header)))]
    screen[window.height-1:window.height, 0:screen_data.footer.width] = [screen_data.footer]

    count_str = bold(blue("Count: " + str(len(screen_data.data))))
    screen[window.height-1:window.height, window.width-count_str.width:window.width] = [count_str]

    screen_pos = 1
    data_idx = 0
    if screen_data.selected > screen_data.startidx + window.height-3:
        screen_data.startidx = screen_data.selected
    if screen_data.selected < screen_data.startidx:
        screen_data.startidx = screen_data.selected

    for item in screen_data.data:
        if data_idx < screen_data.startidx:
            data_idx += 1
            continue

        width = min(window.width, len(item))
        item = item[:width]
        if screen_pos == window.height-1:
            break
        if data_idx == screen_data.selected:
            item = bold(green(item))
        else:
            item = black(item)
        screen[screen_pos:screen_pos+1, 0:width] = [item]
        screen_pos += 1
        data_idx += 1
    window.render_to_terminal(screen)

def table_to_records(table_screen):
    oid = table_screen.raw_data[table_screen.selected].data['TableOid']
    record_data = ogskvquery.query(table_oid=oid, database="template1")
    records_win = ScreenData(get_datalines(record_data), get_header(record_data))
    records_win.raw_data = record_data
    return records_win

def table_to_index_mode(table_screen):
    base = table_screen.raw_data[table_screen.selected]
    isbase = not base.data['IsIndex']
    if not isbase:
        return None
    imode_records = []
    imode_records.append(base)
    for r in table_screen.raw_data:
        if base.data["TableId"] == r.data["BaseTableId"]:
            imode_records.append(r)
    imode_screen = ScreenData(get_datalines(imode_records), get_header(imode_records))
    imode_screen.raw_data = imode_records
    imode_screen.select_func = table_to_records
    return imode_screen

def db_to_tables(db_screen):
    db_name = db_screen.raw_data[db_screen.selected].data['DatabaseName']
    table_data = ogskvquery.get_tables(None, "template1")
    table_win = ScreenData(get_datalines(table_data), get_header(table_data))
    table_win.raw_data = table_data
    table_win.select_func = table_to_records
    return table_win

def to_full_record(screen):
    if screen.raw_data is None or len(screen.raw_data) == 0:
        return None
    record = screen.raw_data[screen.selected]
    lines = []
    for col in record.data.keys():
        lines.append(col + ": " + str(record.data[col]))
    full_win = ScreenData(lines, underline(bold(blue("Single record view"))))
    return full_win

parser = argparse.ArgumentParser()
parser.add_argument("--http", default="http://172.17.0.1:30000", help="HTTP API URL")
args = parser.parse_args()
ogskvquery.cl = SKVClient(args.http)
dbs = ogskvquery.get_databases()
db_win = ScreenData(get_datalines(dbs), get_header(dbs))
db_win.raw_data = dbs
cur_win = db_win
cur_win.select_func = db_to_tables
win_stack = [cur_win]

with FullscreenWindow() as window:
    with Input() as input_generator:
        refresh_screen(window, cur_win)
        for c in input_generator:
            if c == 'q':
                break
            elif c == '<DOWN>' or c == 'j':
                if cur_win.selected < len(cur_win.data) - 1:
                    cur_win.selected += 1
            elif c == '<UP>' or c == 'k':
                if cur_win.selected > 0:
                    cur_win.selected -= 1
            elif c == '<Ctrl-j>': # Return key
                if cur_win.select_func is None:
                    continue
                new_win = cur_win.select_func(cur_win)
                cur_win = new_win
                win_stack.append(new_win)
            elif c == 'f':
                new_win = to_full_record(cur_win)
                if new_win is None:
                    continue
                cur_win = new_win
                win_stack.append(new_win)
            elif c == 'i':
                new_win = table_to_index_mode(cur_win)
                if new_win is None:
                    continue
                cur_win = new_win
                win_stack.append(new_win)
            elif c == '<ESC>':
                win_stack = win_stack[:len(win_stack)-1]
                if len(win_stack) == 0:
                    break
                cur_win = win_stack[len(win_stack)-1]
            refresh_screen(window, cur_win)
