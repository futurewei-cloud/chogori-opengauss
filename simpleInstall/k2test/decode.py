#!python
import re, sys, subprocess
lineparser = re.compile("#\d+ (0x[abcdef\d]+)")
gdbparser = re.compile('(\d+) .* "(.+)" (0x[a-f,0-9]+) (.+)')

def decode(line):
    addrs = lineparser.findall(line)
    binary = sys.argv[1] if len(sys.argv) > 1 else "/opt/opengauss/bin/gaussdb"
    gdbl = "gdb " + binary + ' ' + ' '.join(['-ex "info line *' + hx + '"' for hx in addrs])
    gdbl += ' -ex "quit"'
    gd = subprocess.run(gdbl, shell=True, stdout=subprocess.PIPE, universal_newlines=True).stdout
    
    idx,pos = (0,0)

    while pos >=0:
        idx+=1
        pos = gd.find('Line', pos+1)
        if pos == -1: continue
        st = gd.find('starts', pos)
        e = gd.find('\n', st)
        ln = gd[pos:e].replace('\n', "").replace("   ", " ").replace("starts at address ", "")
        fields = gdbparser.search(ln).groups()
        print (f"#{idx} {fields[2]} in {fields[3]} at {fields[1]}:{fields[0]}")

if __name__ == "__main__":
    for line in sys.stdin:
        decode(line)

