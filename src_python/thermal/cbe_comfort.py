"""
This is a python version of the CBE Comfort tool comfort models.
Also included are functions for polynomial approximations of Universal
Thermal Climate Index (UTCI) for outdoor comfort. The module also includes
functions to calculate humidity ratio and enthalpy from EWP variables so that
these can be used to generate psychrometric charts.
@author Chris Mackey <Chris@MackeyArchitecture.com>
"""

import math

def comfAdaptiveComfortASH55( ta, tr, runningMean, vel=0.6, eightyOrNinety=False, levelOfConditioning=0):
    # Define the variables that will be used throughout the calculation.
    r = []
    coolingEffect = 0
    if eightyOrNinety == True: offset = 3.5
    else: offset = 2.5
    to = (ta + tr) / 2
    # See if the running mean temperature is between 10 C and 33.5 C (the range where the adaptive model is supposed to be used).
    if runningMean >= 10.0 and runningMean <= 33.5:

        if (vel >= 0.6 and to >= 25):
            # calculate cooling effect of elevated air speed
            # when top > 25 degC.
            if vel < 0.9: coolingEffect = 1.2
            elif vel < 1.2: coolingEffect = 1.8
            elif vel >= 1.2: coolingEffect = 2.2
            else: pass

        # Figure out the relation between comfort and outdoor temperature depending on the level of conditioning.
        if levelOfConditioning == 0: tComf = 0.31 * runningMean + 17.8
        elif levelOfConditioning == 1: tComf = 0.09 * runningMean + 22.6
        else: tComf = ((0.09 * levelOfConditioning) + (0.31 * (1 - levelOfConditioning))) * runningMean + ((22.6 * levelOfConditioning) + (17.8 * (1 - levelOfConditioning)))

        tComfLower = tComf - offset
        tComfUpper = tComf + offset + coolingEffect
        r.append(tComf)
        r.append(to - tComf)
        r.append(tComfLower)
        r.append(tComfUpper)

        # See if the conditions are comfortable.
        if to > tComfLower and to < tComfUpper:
            # compliance
            acceptability = True
        else:
            # nonCompliance
            acceptability = False
        r.append(acceptability)

        # Append a number to the result list to show whether the values are too hot, too cold, or comfortable.
        if acceptability == True: r.append(1) # TODO we change from 3claasifers into 2 classifiers
        elif to > tComfUpper: r.append(0)
        else: r.append(0)

    elif runningMean < 10.0:
        # The prevailing temperature is too cold for the adaptive standard but we will use some correlations from adaptive-style surveys of conditioned buildings to give a good guess.
        if levelOfConditioning == 0: tComf = 24.024 + (0.295 * (runningMean - 22.0)) * math.exp((-1) * (((runningMean - 22) / (33.941125)) * ((runningMean - 22) / (33.941125))))
        else:
            conditOffset = 2.6 * levelOfConditioning
            tComf = conditOffset + 24.024 + (0.295 * (runningMean - 22.0)) * math.exp((-1) * (((runningMean - 22) / (33.941125)) * ((runningMean - 22) / (33.941125))))

        tempDiff = to - tComf
        tComfLower = tComf - offset
        tComfUpper = tComf + offset
        if to > tComfLower and to < tComfUpper: acceptability = True
        else: acceptability = False
        if acceptability == True: condit = 1# 0 # TODO we change from 3claasifers into 2 classifiers
        elif to > tComfUpper: condit = 0# 1
        else: condit = 0# -1
        outputs = [tComf, tempDiff, tComfLower, tComfUpper, acceptability, condit]
        r.extend(outputs)
    else:
        # The prevailing temperature is too hot for the adaptive method.
        # This should usually not happen for climates on today's earth but it might be possible in the future with global warming.
        # For this case, we will just use the adaptive model at its hottest limit.
        if (vel >= 0.6 and to >= 25):
            if vel < 0.9: coolingEffect = 1.2
            elif vel < 1.2: coolingEffect = 1.8
            elif vel >= 1.2: coolingEffect = 2.2
            else: pass
        if levelOfConditioning == 0: tComf = 0.31 * 33.5 + 17.8
        else: tComf = ((0.09 * levelOfConditioning) + (0.31 * (1 - levelOfConditioning))) * 33.5 + ((22.6 * levelOfConditioning) + (17.8 * (1 - levelOfConditioning)))
        tempDiff = to - tComf
        tComfLower = tComf - offset
        tComfUpper = tComf + offset + coolingEffect
        if to > tComfLower and to < tComfUpper: acceptability = True
        else: acceptability = False
        if acceptability == True: condit = 1 # 0
        elif to > tComfUpper: condit = 0 # 1
        else: condit = 0#-1 TODO we change from 3claasifers into 2 classifiers
        outputs = [tComf, tempDiff, tComfLower, tComfUpper, acceptability, condit]
        r.extend(outputs)

    return r



if __name__ == "__main__":
    # We modify the code into comfort VS un-comfort classifier
    print(comfAdaptiveComfortASH55(29, 27, 27)[5])