############################################
############################################
#### LUSIR TRANSCRIPT CORRECTION TOOL 4 ####
############################################
############################################


def lusir_transcript_correction_list_odfpy_4(source):
    import re
    import io
    import os
    import pandas as pd
    from pandas import DataFrame
    from pyexcel_ods3 import save_data
    from collections import OrderedDict
    from timecode_manipulation import timecode_median, timecode_median_multi, timecode_to_frames, frames_to_timecode


    logfile = ''
    auftragsbuch = ''
    file = pd.read_excel(source, engine="odf")

    ############################################################
    ## Leere/falsche Zeilen und Spalten entfernen/korrigieren ##
    ############################################################

    file.reset_index(drop=True)  # Index neu setzen

    sprecherspalte = False
    print(sprecherspalte)

    for i in range(10):
        try:
            file.pop('Unnamed: '+ str(i))
        except KeyError:
            print('keine leere Spalte '+str(i))

    speaker_names = ['SPEAKER', 'Speaker', 'speaker', 'Sprecher', 'SPRECHER', 'sprecher']

    for i, speaker_name in enumerate(speaker_names):
        try:
            file[speaker_name] = file[speaker_name]
            sprecherspalte = True
            break
        except KeyError:
            if i < len(speaker_names):
                print(str(i), speaker_name)
                continue
            else:
                auftragsbuch = auftragsbuch + 'Srecherspalte nicht gefunden: ' + source + '\n'
        except ValueError:  # falls falsche Datentypen in den Zeilen versteckt sind, funktioniert die parallele Verarbeitung nicht
            print(speaker_name)
            file_list_parser = file.values.tolist()
            if file_list_parser[0][1] not in speaker_names:
                sprecherspalte == False
                file = DataFrame(file_list_parser, columns=['Timecode', 'Transcript'])
            if file_list_parser[0][1] in speaker_names:
                sprecherspalte == True
                speaker_name = file_list_parser[0][1]
                file = DataFrame(file_list_parser, columns=['Timecode', speaker_name, 'Transcript'])


    ##########################################################################
    ## Formatierung vereinheitlichen und Transkriptionszeichen herausparsen ##
    ##########################################################################
    transcript_names = ['Transkript', 'transkript', 'TRANSKRIPT', 'Transcript', 'transcript', 'TRANSCRIPT']

    for i, transcript_name in enumerate(transcript_names):
        try:
            file[transcript_name] = file[transcript_name].str.replace('{', '')
            file[transcript_name] = file[transcript_name].str.replace('}', '')
            break
        except KeyError:
            if i < len(transcript_names):
                print(str(i), transcript_name)
                continue
            else:
                auftragsbuch = auftragsbuch + 'Spalte Transkript nicht gefunden: ' + source + '\n'
                return auftragsbuch
        except ValueError:  # falls falsche Datentypen in den Zeilen versteckt sind, funktioniert die parallele Verarbeitung nicht
            transcript_name = 'TRANSCRIPT'
            file_list_parser = file.values.tolist()
            for line in file_list_parser:
                try:
                    line[1] = line[1].replace('{', '').replace('}', '')
                except AttributeError:  # falsche Datentypen überspringen, z.B. Zeitstempel, die keine Bedeutung haben
                    continue
                except ValueError:  # am Ende der gefüllten Spalten beenden (es folgen nan = floats)
                    break
            if sprecherspalte == False:
                file = DataFrame(file_list_parser, columns=['Timecode', transcript_name])
            if sprecherspalte == True:
                file = DataFrame(file_list_parser, columns=['Timecode', speaker_name, transcript_name])


    timecode_names = ['Timecode', 'timecode', 'TIMECODE', 'IN', 'In', 'in']

    for i, timecode_name in enumerate(timecode_names):
        try:
            file[timecode_name] = file[timecode_name].str.replace(',', '.')
            file[timecode_name] = file[timecode_name].str.replace('[', '')
            file[timecode_name] = file[timecode_name].str.replace(']', '')
            break
        except KeyError:
            if i < len(timecode_names):
                print(str(i), timecode_name)
                continue
            else: # Spalte Transkript nicht benannt, Weiterverarbeitung nicht möglich
                auftragsbuch = auftragsbuch + 'Spalte Timecode nicht gefunden: ' + source + '\n'
                return auftragsbuch
        except AttributeError:  # in einigen dokumenten sind die Timecodes als Datetime-Objekt angegeben und müssen in einen String umgewandelt werden
            timecode_name = 'IN'
            file_list = file.values.tolist()
            logfile = logfile + 'Timecode als Datetime in Excel\n'
            for id, line in enumerate(file_list):
                if type(line[0]) is float:  # leere Zeilen (=nan =float) überspringen
                    continue
                else:
                    if not sprecherspalte:
                        tc_new = [line[0].strftime('%H:%M:%S.%f')[:-4], line[1]]
                    if sprecherspalte:
                        tc_new = [line[0].strftime('%H:%M:%S.%f')[:-4], line[1], line[2]]
                    file_list.pop(id)
                    file_list.insert(id, tc_new)
            if sprecherspalte == False:
                file = DataFrame(file_list, columns=[timecode_name, transcript_name])
            if sprecherspalte == True:
                file = DataFrame(file_list, columns=[timecode_name, speaker_name, transcript_name])




    file[transcript_name] = file[transcript_name].str.replace(':\*',
                                                        '*')  # Doppelpunkte vor Asterisk raus; Breakout vor Asterisk in Replace!!!
    file[transcript_name] = file[transcript_name].str.replace('\*:',
                                                        '*')  # Doppelpunkte nach Asterisk raus; Breakout vor Asterisk in Replace!!!

    if type(file.at[0, timecode_name]) is float:  # Wenn erster TC fehlt: 00:00:00.01 setzen
        file.at[0, timecode_name] = '00:00:00.01'
        logfile = logfile + 'Erster TC neu gesetzt\n'

    if type(file.at[0, timecode_name]) is str and len(
            file.at[0, timecode_name]) < 10:  # Wenn erste Zeile ein Kolumnenindex ist: entfernen
        file.drop(0, inplace=True)
        logfile = logfile + 'Indexzeile gelöscht\n'

    # -> DF neu indizieren?

    ##################################
    ## Dataframe in Liste umwandeln ##
    ##################################

    file_list = file.values.tolist()
    new_file_list = []
    print(file_list[:9])
    ######################################
    ## Timecode-Chronologie korrigieren ##
    ######################################

    if sprecherspalte == True:
        transcript_column_index = 2
    if sprecherspalte == False:
        transcript_column_index = 1



    file_list_2 = []
    for line in file_list:  # leere Zeilen entfernen
        if type(line[transcript_column_index]) is float:
            continue
        else:
            if sprecherspalte:
                file_list_2.append(line[:3])
            if not sprecherspalte:
                file_list_2.append(line[:2])

    print(file_list_2[:9])
    frames_set = 0
    for line in file_list_2:  # Framerate bestimmen
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

    for id, line in enumerate(file_list_2):  # über alle Zeilen iterieren, Timecode ist in erster (= nullter) Spalte
        tc = line[0]

        if id <= len(file_list_2) - 2:  # der jeweils nächste Timecode zur Berechnung eines Medians, sollte einer fehlen
            next_tc = file_list_2[id + 1][0]
        else:
            next_tc = next_tc

        if type(line[transcript_column_index]) is float or line[transcript_column_index].isspace():  # Zeilen ohne Transkription erkennen
            if type(line[0]) is str:
                auftragsbuch = auftragsbuch + 'Keine Transkription in Spalte: ' + source + str(line[0]) + '\n'
            continue

        if type(tc) is str:  # wenn ein TC vorhanden, ist es ein String aus Zahlen
            frames = timecode_to_frames(tc,
                                        framerate)  # scheinbar sind in manchen Dokumenten einstellige Timecodes ohne Null angegeben, das wirft error
            if frames <= tc_frames_set:  # außerdem: scheinbar Unicode Errors in manchen Doks in der TC-SPlate
                if timecode_to_frames(next_tc, framerate) > tc_frames_set:
                    try:
                        tc_new = timecode_median(frames_to_timecode(tc_frames_set, framerate), next_tc, framerate)
                        tc_frames_set = timecode_to_frames(tc_new, framerate)
                        if sprecherspalte:
                            new_file_list.append([tc_new, line[1], line[2]])
                        if not sprecherspalte:
                            new_file_list.append([tc_new, line[1]])
                        logfile = logfile + tc_new + " neu gesetzt (alt: " + frames_to_timecode(frames,
                                                                                                framerate) + ')\n'
                    except ValueError:
                        auftragsbuch = auftragsbuch + "Differenz zu gering, Bildung eines Medians nicht möglich: " + source + ' ' + str(
                            line[0]) + ', ' + file_list[id + 1][0] + '\n'
                        continue
                elif timecode_to_frames(next_tc, framerate) <= tc_frames_set:
                    auftragsbuch = auftragsbuch + 'Mehrere falsche Timecodes in Folge: ' + source + ' ' + str(
                        line[0]) + '\n'
                    return auftragsbuch
            if frames > tc_frames_set:
                tc_frames_set = frames
                new_file_list.append(line)



        if type(
                tc) is float:  # wenn kein TC eingetragen, ist es ein NaN (dataframespezifisch) und das ist wiederum ein float (?!)
            if type(next_tc) is float and id != len(file_list_2):
                auftragsbuch = auftragsbuch + 'Mehrere fehlende Timecodes in Folge: ' + source + ' ' + str(
                    line[0]) + '\n'
                return auftragsbuch
            elif type(next_tc) is float and id == len(file_list_2):
                auftragsbuch = auftragsbuch + 'Letzter Timecode fehlt: ' + source + '\n'
                break
            else:
                tc_new = timecode_median(frames_to_timecode(tc_frames_set, framerate), next_tc, framerate)
                tc_frames_set = timecode_to_frames(tc_new, framerate)
                new_file_list.append([tc_new, line[1], line[2]])
                logfile = logfile + str(tc_new) + " neu gesetzt, vorher None\n"

    ############################################################################################################
    ## In Spalte Transkript: wenn Anzahl Sternchen eins: Fehlermeldung; wenn Anzahl Sternchen >2 und gerade:
    ## vor jedem ungeraden Sternchen cut und den folgenden Inhalt in eine neue Zeile einfügen,
    ############################################################################################################

    ###########################################################################
    ## Zunächst Sprecherliste für Dokument erstellen und ausgeben für Upload ##
    ###########################################################################

    if not sprecherspalte:

        file_list_final = []
        sprecher_doc = []
        for ID, line in enumerate(new_file_list):

            if len(re.findall(r"\*", line[1])) % 2 == 0:  # wenn Anzahl Asterisks gerade
                sprecher_check = re.findall(r"\*([0-9A-Za-zäÄöÖüÜß. ]+?)\*",
                                            line[1])  # Mit Regex alle Sprecher in der Zeile finden
                for i in sprecher_check:
                    if i not in sprecher_doc:
                        sprecher_doc.append(i)  # Sprecherliste für Dokument aufbauen

            if len(re.findall(r"\*", line[1])) % 2 != 0:  # Wenn anzahl Asterisks ungerade
                if not '<***>' in line[1]:  # falls nicht Auszeichnung für Bandende (<***>) in Zeile
                    for i in sprecher_doc:
                        if '*' + i + ' ' in line[1]:
                            line[1] = line[1].replace('*' + i, '*' + i + '*')  # schließendes Asterisk ergänzen
                            continue
                        if '*' + i + ':' in line[1]:
                            line[1] = line[1].replace('*' + i + ':',
                                                      '*' + i + '*')  # schließendes Asterisk ergänzen, Doppelpunkt entfernen
                            continue
                        if i + '*' in line[1]:
                            line[1] = line[1].replace(i + '*', '*' + i + '*')  # öffnendes Asterisk ergänzen
                            continue

        if len(sprecher_doc) < 2:
            print('Zu wenige Sprecher erkannt, bitte überprüfen!')
            auftragsbuch = auftragsbuch + 'Zu wenige Sprecher erkannt, bitte überprüfen: ' + source

        # Liste mit Sprechern ausgeben

        out = open(source[:-4] + '_SPRECHER.txt', 'w', encoding='UTF-8')
        out.write(str(sprecher_doc))
        out.close()

        for ID, line in enumerate(new_file_list):
            if len(re.findall(r"\*", line[1])) % 2 == 0:  # wenn Anzahl Asterisks gerade

                sprecher = re.findall(r"\*([0-9A-Za-zäÄöÖüÜß. ]+?)\*",
                                      line[1])  # Mit Regex alle Sprecher in der Zeile finden
                sequenz = re.split(r"\*([0-9A-Za-zäÄöÖüÜß. ]+?)\*",
                                   line[1])  # Zeile nach den Sprechern aufsplitten und Ergebnis in Liste speichern

                if sequenz[0] == '':
                    sequenz.pop(0)

                if len(sprecher) == 1 and sequenz[0] in sprecher:  # Ein Sprecher, Sprecher steht am Anfang der Zeile
                    file_list_final.append([ID, line[0], sequenz[0], sequenz[1]])
                    continue

                elif len(sprecher) > 1 and sequenz[
                    0] in sprecher:  # Mehrere Sprecher, ein Sprecher steht am Anfang der Zeile
                    timecodes = timecode_median_multi(line[0], new_file_list[ID + 1][0], framerate, len(sprecher))
                    for i in range(len(sprecher)):
                        file_list_final.append([ID, timecodes[i], sequenz[0], sequenz[
                            1]])  # <- Median: durch anzahl teilen und jew. 1/xtel mehr addieren?
                        sequenz.pop(0)
                        sequenz.pop(0)
                        logfile = logfile + 'Sprecherwechsel eingefügt: ' + timecodes[i] + '\n'
                    continue

                elif len(sprecher) > 1 and sequenz[
                    0] not in sprecher:  # wenn mehrere Sprechwechsel, aber keiner am Anfang der Zeile
                    file_list_final.append([ID, line[0], '§$%', sequenz[0]])
                    sequenz.pop(0)  # ersten Satz aus Sequenz entfernen
                    timecodes = timecode_median_multi(line[0], new_file_list[ID + 1][0], framerate, len(sprecher) + 1, 1)
                    for i in range(len(sprecher)):  # <- funktioniert die for Schleife?
                        file_list_final.append([ID, timecodes[i], sequenz[0], sequenz[
                            1]])  # <- Median: durch anzahl teilen und jew. 1/xtel mehr addieren?
                        sequenz.pop(0)
                        sequenz.pop(0)
                        logfile = logfile + 'Sprecherwechsel eingefügt: ' + timecodes[i] + '\n'
                    continue

                elif len(sprecher) == 1 and sequenz[0] not in sprecher and sequenz[
                    2] not in sprecher:  # Ein Sprecher, Sprecher steht
                    file_list_final.append([ID, line[0], '§$%', sequenz[0]])  # mitten in der Zeile
                    try:
                        file_list_final.append(
                            [ID, timecode_median(line[0], new_file_list[ID + 1][0], framerate), sequenz[1],
                             sequenz[2]])  # <- neuer Timecode: Median setzen!
                        logfile = logfile + 'Sprecherwechsel eingefügt: ' + timecode_median(line[0],
                                                                                            new_file_list[ID + 1][0],
                                                                                            framerate) + '\n'
                    except IndexError:
                        file_list_final.append([ID, '???', sequenz[1], sequenz[2]])
                        auftragsbuch = auftragsbuch + 'Letzten TC setzen: ' + source
                    continue

                elif len(sprecher) == 0:  # kein Sprecher in Zeile
                    file_list_final.append(
                        [ID, line[0], '§$%', sequenz[0]])  # Problem könnte sein, dass auch leere Zeilen hier reinkommen
                    continue

            if len(re.findall(r"\*", line[1])) % 2 != 0:  # Wenn anzahl Asterisks immer noch ungerade

                if '<***>' in line[1] and len(re.findall(r"\*", line[1])) > 3:

                    sprecher = re.findall(r"\*([0-9A-Za-zäÄöÖüÜß. ]+?)\*",
                                          line[1])  # Mit Regex alle Sprecher in der Zeile finden
                    sequenz = re.split(r"\*([0-9A-Za-zäÄöÖüÜß. ]+?)\*",
                                       line[1])  # Zeile nach den Sprechern aufsplitten und Ergebnis in Liste speichern

                    if sequenz[0] == '':
                        sequenz.pop(0)

                    if len(sprecher) == 1 and sequenz[0] in sprecher:  # Ein Sprecher, Sprecher steht am Anfang der Zeile
                        file_list_final.append([ID, line[0], sequenz[0], sequenz[1]])
                        continue

                    elif len(sprecher) > 1 and sequenz[
                        0] in sprecher:  # Mehrere Sprecher, ein Sprecher steht am Anfang der Zeile
                        timecodes = timecode_median_multi(line[0], new_file_list[ID + 1][0], framerate, len(sprecher))
                        for i in range(len(sprecher)):
                            file_list_final.append([ID, timecodes[i], sequenz[0], sequenz[
                                1]])  # <- Median: durch anzahl teilen und jew. 1/xtel mehr addieren?
                            sequenz.pop(0)
                            sequenz.pop(0)
                            logfile = logfile + 'Sprecherwechsel eingefügt: ' + timecodes[i] + '\n'
                        continue

                    elif len(sprecher) > 1 and sequenz[
                        0] not in sprecher:  # wenn mehrere Sprechwechsel, aber keiner am Anfang der Zeile
                        file_list_final.append([ID, line[0], '§$%', sequenz[0]])
                        sequenz.pop(0)  # ersten Satz aus Sequenz entfernen
                        timecodes = timecode_median_multi(line[0], new_file_list[ID + 1][0], framerate, len(sprecher) + 1,
                                                          1)
                        for i in range(len(sprecher)):  # <- funktioniert die for Schleife?
                            file_list_final.append([ID, timecodes[i], sequenz[0], sequenz[
                                1]])  # <- Median: durch anzahl teilen und jew. 1/xtel mehr addieren?
                            sequenz.pop(0)
                            sequenz.pop(0)
                            logfile = logfile + 'Sprecherwechsel eingefügt: ' + timecodes[i] + '\n'
                        continue

                    elif len(sprecher) == 1 and sequenz[0] not in sprecher and sequenz[
                        2] not in sprecher:  # Ein Sprecher, Sprecher steht
                        file_list_final.append([ID, line[0], '§$%', sequenz[0]])  # mitten in der Zeile
                        try:
                            file_list_final.append(
                                [ID, timecode_median(line[0], new_file_list[ID + 1][0], framerate), sequenz[1],
                                 sequenz[2]])
                            logfile = logfile + 'Sprecherwechsel eingefügt: ' + timecode_median(line[0],
                                                                                                new_file_list[ID + 1][0],
                                                                                                framerate) + '\n'
                        except IndexError:
                            file_list_final.append([ID, '???', sequenz[1], sequenz[2]])
                            auftragsbuch = auftragsbuch + 'Letzten TC setzen: ' + source
                        continue

                if '<***>' in line[1] and len(re.findall(r"\*", line[1])) == 3:
                    file_list_final.append([ID, line[0], '#', line[1]])
                    continue
                else:
                    file_list_final.append([ID, line[0], '', line[1]])
                    auftragsbuch = auftragsbuch + 'ungerade Anzahl Asterisks, prüfen: ' + source + ' ' + str(line[0]) + '\n'
                    continue

        #########################################################
        ## Leerzeichen am Anfang der Transkriptzeile entfernen ##
        ## wenn am Anfang der Transkriptzeile eine Zahl steht, ##
        ## produziert das beim Schreiben des ODS einen Fehler, ##
        ## daher muss ein Zeichen davor gesetzt werden         ##
        #########################################################

        for line in file_list_final:
            if len(line[3]) > 1:
                while line[3][0] == ' ':
                    line[3] = line[3][1:]

        ######################################################################
        ## Chronologie anhand der Reihenfolge der ursprünglichen IDs prüfen ##
        ######################################################################

        limit = 0
        for count, line in enumerate(file_list_final):
            if limit < len(file_list_final) - 1:
                if file_list_final[count + 1][0] - line[0] > 1:
                    auftragsbuch = auftragsbuch + 'Fehler in der Reihenfolge der IDs: ' + source + ', Zeile: ' + str(
                        count + 2) + '\n\n'  # -> Auftragsbuch
                    limit = limit + 1
                else:
                    limit = limit + 1
                    continue

        #########################
        ## Dataframe erstellen ##
        #########################
        file_new = DataFrame(file_list_final, columns=['Index', 'IN', 'SPEAKER', 'TRANSCRIPT'])

        ###################################################################################
        ## Leere Zeilen in Spalte Sprecher mit dem jeweils darüber liegenden Wert füllen ##
        ###################################################################################

        for id, line in enumerate(file_new['SPEAKER']):
            if id > 0 and '§$%' in line:
                file_new.at[id, 'SPEAKER'] = file_new.at[id - 1, 'SPEAKER']
            if id == 0 and '§$%' in line:
                auftragsbuch = auftragsbuch + 'Kein vorheriger Sprecher lokalisierbar, bitte 1. Sprecher einfügen: ' + source + '\n\n'

        file_new.pop('Index')

    if sprecherspalte:
        file_new = DataFrame(new_file_list, columns=['IN', 'SPEAKER', 'TRANSCRIPT'])

    ####################
    ## Datei ausgeben ##
    ####################

    # korrigiertes Transkript

    odf_list = [file_new.columns.values.tolist()] + file_new.values.tolist()

    data = OrderedDict()  #
    data.update({"Sheet 1": odf_list})
    save_data(source[:-4] + '_NEW.ods', data)

    # logfile

    out = open(source[:-4] + '_LOGFILE.txt', 'w', encoding='UTF-8')
    out.write(logfile)
    out.close()


    return auftragsbuch


########################################################################################################################
########################################################################################################################
########################################################################################################################


import os

Auftragsbuch = '#############################################################################################################\n' \
               'Das ist eine Auflistung von Jobs, die händisch korrigiert werden müssen.\n\n' \
               'Timecodes: sollten mehrere falsche oder fehlende Timecodes aufeinander folgen, der letzte Timecode\n' \
               'fehlen oder ein neuer Timecode nicht errechnet werden, weil die Differenz zwischen dem vorherigen\n' \
               'und dem nächsten zu gering ist, kann keine automatische Timecodekorrektur angewandt werden.\n' \
               'In dem Fall muss das Ursprungsdokument, das angezeigt wird, zunächst händisch korrigiert und\n' \
               'dann erneut durch das Programm geschleift werden.\n\n' \
               'Sprecherwechsel: wenn kein Sprecher lokalisierbar war (markiert durch §$%),\n' \
               'bitte händisch in das neue Dokument eintragen. Wenn der Sprechwechsel aufgrund fehlender Sternchen nicht\n' \
               'zugeordnet werden konnte (markiert durch ###), bitte händisch im neuen Dokument nachtragen.\n\n' \
               'Fehler in der Reihenfolge der IDs: bitte neues mit altem Dokument abgleichen, ob Zeilen verschwunden sind\n' \
               '##############################################################################################################\n\n\n'

path = '' # Pfad zu den ods-Dateien

for file in os.listdir(path):
    if file.endswith('.ods'):
        print(file)
        Auftragsbuch = Auftragsbuch + lusir_transcript_correction_list_odfpy_4(path + '\\'+file)
        out_auftragsbuch = open(path +'\\Auftragsbuch.txt', 'w', encoding='UTF-8')
        out_auftragsbuch.write(Auftragsbuch)

        out_auftragsbuch.close()
