import aipy
import numpy as np

def readXdata(filename):
    ibl = np.array(
       [[0,0,1,2,0,0,0,0],
       [0,0,3,4,0,0,0,0],
       [0,0,0,5,0,0,0,0],
       [0,0,0,0,0,0,0,0],
       [0,0,0,0,0,6,7,8],
       [0,0,0,0,0,0,9,10],
       [0,0,3,4,0,0,0,11]])
    # Open uv file for reading
    uv = aipy.miriad.UV(filename)
    # Read one record to get number of good frequencies
    preamble, data = uv.read()
    uv.rewind()
    nf = len(data.nonzero()[0])
    freq = uv['sfreq'][data.nonzero()[0]]
    out = np.zeros((12,nf,600,2),dtype=np.complex64)
    l = -1
    tprev = 0
    for preamble, data in uv.all():
        uvw, t, (i,j) = preamble
        if uv['pol'] == -2:
            k = 1
        else:
            k = 0
        if t != tprev:
            # New time 
            l += 1
            tprev = t
            if l == 600:
                break
        if len(data.nonzero()[0]) == nf:
            out[ibl[i,j],:,l,k] = data[data.nonzero()]
    return out, freq

import copy
from . import spectrogram_fit as sp
from .util import Time

trange = Time(['2015-06-21 01:26:00','2015-06-21 01:46:00'])
s = sp.Spectrogram(trange)
s.fidx = [0,213]
tsys, std = s.get_median_data()

out,fghz = readXdata('/data1/IDB/IDB20150621012612')
out2,f = readXdata('/data1/IDB/IDB20150621013612')
out = concatenate((out,out2),2)
pcal = angle(out[:,:,550,:])
acal = abs(out[:,:,550,:])
calout = copy.copy(out)
# Calibrate for time 550, just before the peak of the flare.
for i in range(1200):
    calout[:,:,i,:] = calout[:,:,i,:]*(cos(pcal)-1j*sin(pcal))/acal

# Normalize to the total power spectrum at the same time, with reference
# to the shortest baseline (preserves the relative amplitudes on various
# baselines and polarizations.
norm = abs(calout[:,:,550,:])
for i in range(12):
    for j in range(2):
        norm[i,:,j] = tsys[:,550]*abs(calout[i,:,550,j]) / abs(calout[0,:,550,0])

for i in range(1200):
    calout[:,:,i,:] = calout[:,:,i,:]*norm

# Multi-panel Plot
f, ax = subplots(4,5)

sbl = ['1-2','1-3','1-4','2-3','2-4','3-4','5-7','5-8','7-8']
for i,ibl in enumerate([0,1,2,3,4,5,7,8,11]):
    if (i > 4):
        ax[2,i % 5].imshow(  abs(calout[ibl,50:,:,0]))
        ax[2,i % 5].text(100,10,sbl[i]+' Amp',color='white')
        ax[3,i % 5].imshow(angle(calout[ibl,50:,:,0]))
        ax[3,i % 5].text(100,10,sbl[i]+' Phase')
    else:
        ax[0,i % 5].imshow(  abs(calout[ibl,50:,:,0]))
        ax[0,i % 5].text(100,10,sbl[i]+' Amp',color='white')
        ax[1,i % 5].imshow(angle(calout[ibl,50:,:,0]))
        ax[1,i % 5].text(100,10,sbl[i]+' Phase')

ax[2,4].imshow(tsys[50:,:])
ax[2,4].text(100,10,'Total Power',color='white')

# Saturation Plot
figure()
plot(tsys[100,:],abs(calout[11,100,:,0]),'.',label=str(fghz[100])[:5]+' GHz')
plot(tsys[150,:],abs(calout[11,150,:,0]),'.',label=str(fghz[150])[:5]+' GHz')
plot(tsys[200,:],abs(calout[11,200,:,0]),'.',label=str(fghz[200])[:5]+' GHz')
xlabel('Total Power [sfu]')
ylabel('Correlated Power (Ants 7-8) [sfu]')
legend(loc='lower right')
title('EOVSA Flare 2015-06-21')
