#!/usr/bin/env python

import sys, string

def parse(keyfile, search=None):
    dot = ""
    lines = ""
    
    existing = []

    fp = open(keyfile)
    lines += "strict digraph keys {\n"
    lines += "rankdir=LR;\n"
    lines += "graph [bgcolor=black];\n"
    lines += 'edge [color="darkorchid4"];\n'
    lines += 'node [color="darkorchid4"];\n'

    for line in fp.readlines():
        if (search is not None) and (search not in line):
            continue

        chunks = line.split("/")[1:]
        
        count = 1
        while count < len(chunks) - 1:
            if count == 0:
                count +=1
                continue
            else:
                src_label = chunks[count - 1].strip()
                dest_label = chunks[count].strip()

            src_id = string.join(chunks[0:count], "_")
            dest_id = string.join(chunks[0:count + 1], "_")

            if src_label[0].isdigit() or dest_label[0].isdigit():
                count += 1
                continue
                
            id_line = src_id + " -> " + dest_id + "\n"
            src_label_line = src_id + ' [label="%s" fontcolor="chartreuse" penwidth="4.0"];\n' % src_label
            dest_label_line = dest_id + ' [label="%s" fontcolor="chartreuse" penwidth="4.0"];\n' % dest_label


            if id_line not in lines:
                lines += id_line

            if src_label_line not in lines:
                lines += src_label_line

            if dest_label_line not in lines:
                lines += dest_label_line

            count += 1
            
        
    lines += "}\n"

    return lines
        

def main():
    keyfile = sys.argv[1]
    if len(sys.argv) > 2:
        search = sys.argv[2]
    else:
        search = None

    dot = parse(keyfile, search)
    print dot

if __name__ == "__main__":
    main()
