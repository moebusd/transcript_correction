
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

