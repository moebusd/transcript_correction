#############################################################################
### Hier den Pfad zu einem Ordner mit ods-Dateien nach OHD-Schema angeben ###
#############################################################################

path = 'C:\\Users\\moebusd\\sciebo\\OHD\\Transkriptkorrektur\\Demo FU'


################################################
### Defining Timecode Manipulation Functions ###
################################################


def timecode_median(tc1, tc2, framerate):
    difference = timecode_to_frames(tc2, framerate) - timecode_to_frames(tc1, framerate)
    if difference < 2:
        raise ValueError("Differenz zu gering, Bildung eines Medians nicht möglich: " + tc1 + ', ' + tc2)

    median = frames_to_timecode(timecode_to_frames(tc1, framerate) + (difference / 2), framerate)

    return median


def timecode_median_multi(tc1, tc2, framerate, divisor, *startingpoint):
    difference = timecode_to_frames(tc2, framerate) - timecode_to_frames(tc1, framerate)
    if difference < divisor:
        raise ValueError("Differenz zu gering, Bildung eines Medians nicht möglich: " + tc1 + ', ' + tc2)

    median_multi = []

    if startingpoint:
        for i in range(startingpoint[0], divisor):
            tc_new = frames_to_timecode(timecode_to_frames(tc1, framerate) + (difference / divisor * i), framerate)
            median_multi.append(tc_new)
    else:
        for i in range(divisor):
            tc_new = frames_to_timecode(timecode_to_frames(tc1, framerate) + (difference / divisor * i), framerate)
            median_multi.append(tc_new)

    return median_multi


def timecode_to_frames(tc, framerate):
    minutes = int(tc[:2]) * 60
    seconds = (minutes + int(tc[3:5])) * 60
    frames = (seconds + int(tc[6:8])) * framerate + int(tc[9:])
    return frames


def frames_to_timecode(frames, framerate):
    tc_fr = int(frames % framerate)
    if tc_fr < 10:
        tc_fr = '0' + str(tc_fr)
    tc_s = int((frames / framerate) % 60)
    if tc_s < 10:
        tc_s = '0' + str(tc_s)
    tc_m = int(frames / framerate / 60 % 60)
    if tc_m < 10:
        tc_m = '0' + str(tc_m)
    tc_h = int(frames / framerate / 60 / 60)
    if tc_h < 10:
        tc_h = '0' + str(tc_h)

    return str(tc_h) + ':' + str(tc_m) + ':' + str(tc_s) + '.' + str(tc_fr)


################################################################################
################################################################################
#### LUSIR TIMECODE CORRECTION TOOL 1 BASED ON TRANSCRIPT CORRECTION TOOL 4 ####
################################################################################
################################################################################


def lusir_timecode_correction_1(source, filename):
    import re
    import io
    import os
    import pandas as pd
    from pandas import DataFrame
    from pyexcel_ods3 import save_data
    from collections import OrderedDict

    logfile = ''
    file = pd.read_excel(source, engine="odf")

    ##################################
    ## Dataframe in Liste umwandeln ##
    ##################################

    file_list = file.values.tolist()
    new_file_list = []
    print(file_list[:9])
    ######################################
    ## Timecode-Chronologie korrigieren ##
    ######################################


    print(file_list[:9])
    frames_set = 0
    for line in file_list:  # Framerate bestimmen
        if type(line[0]) is str and not line[0].isspace():
            if line[0][0] == ' ':
                line[0] = line[0][1:]
            if int(line[0][9:12]) > frames_set:
                frames_set = int(line[0][9:12])
        else:
            continue
    if frames_set > 59:
        framerate = 1000 # dann ist TC in Milisekunden angegeben
    if frames_set == 59:
        framerate = 60
    if frames_set == 29:
        framerate = 30
    if frames_set == 24:
        framerate = 25
    if frames_set < 24:
        framerate = 24


    tc_frames_set = 0  # Timecodemarker setzen, um die Chronologie zu prüfen

    for id, line in enumerate(file_list):  # über alle Zeilen iterieren, Timecode ist in erster (= nullter) Spalte
        tc = line[0]

        if id <= len(file_list) - 2:  # der jeweils nächste Timecode zur Berechnung eines Medians, sollte einer fehlen
            next_tc = file_list[id + 1][0]
        else:
            next_tc = next_tc


        if type(tc) is str:  # wenn ein TC vorhanden, ist es ein String aus Zahlen
            frames = timecode_to_frames(tc, framerate)  # scheinbar sind in manchen Dokumenten einstellige Timecodes ohne Null angegeben, das wirft error
            if frames <= tc_frames_set:  # außerdem: scheinbar Unicode Errors in manchen Doks in der TC-SPlate
                if timecode_to_frames(next_tc, framerate) > tc_frames_set:
                    try:
                        tc_new = timecode_median(frames_to_timecode(tc_frames_set, framerate), next_tc, framerate)
                        tc_frames_set = timecode_to_frames(tc_new, framerate)
                        new_file_list.append([tc_new, line[1], line[2]])
                        logfile = logfile + filename + ' ' + tc_new + " neu gesetzt (alt: " + frames_to_timecode(frames, framerate) + ')' + '\n'
                    except ValueError:
                        logfile = logfile + filename + ' ' + "Differenz zu gering, Bildung eines Medians nicht möglich: " + ' ' + str(
                            line[0]) + ', ' + file_list[id + 1][0] + '\n'
                        continue
                elif timecode_to_frames(next_tc, framerate) <= tc_frames_set:
                    logfile = logfile + filename + ' ' + 'Mehrere falsche Timecodes in Folge: ' + ' ' + str(
                        line[0]) + '\n'
                    return logfile
            if frames > tc_frames_set:
                tc_frames_set = frames
                new_file_list.append(line)



        if type(
                tc) is float:  # wenn kein TC eingetragen, ist es ein NaN (dataframespezifisch) und das ist wiederum ein float (?!)
            if type(next_tc) is float and id != len(file_list):
                logfile = logfile + filename + ' ' + 'Mehrere fehlende Timecodes in Folge: ' + ' ' + str(
                    line[0]) + '\n'
                return logfile
            elif type(next_tc) is float and id == len(file_list):
                logfile = logfile + filename + ' ' + 'Letzter Timecode fehlt: ' + '\n'
                break
            else:
                tc_new = timecode_median(frames_to_timecode(tc_frames_set, framerate), next_tc, framerate)
                tc_frames_set = timecode_to_frames(tc_new, framerate)
                new_file_list.append([tc_new, line[1], line[2]])
                logfile = logfile + filename + ' ' + str(tc_new) + " neu gesetzt, vorher None" + '\n'



    file_new = DataFrame(new_file_list, columns=['IN', 'SPEAKER', 'TRANSCRIPT'])

    ####################
    ## Datei ausgeben ##
    ####################

    # korrigiertes Transkript

    odf_list = [file_new.columns.values.tolist()] + file_new.values.tolist()

    data = OrderedDict()  #
    data.update({"Sheet 1": odf_list})
    save_data(source[:-4] + '_corr.ods', data)

    return logfile


########################################################################################################################
########################################################################################################################
########################################################################################################################


import os

Logfile = ''

for file in os.listdir(path):
    if file.endswith('.ods'):
        print(file)
        Logfile = Logfile + lusir_timecode_correction_1(path + '\\'+file, file)
        out_logfile= open(path +'\\logfile.txt', 'w', encoding='UTF-8')
        out_logfile.write(Logfile)

        out_logfile.close()
