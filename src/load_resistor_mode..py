import numpy as np
from matplotlib import pyplot as plt

def get_ambient_temperature(t: int, T0 = 293, amplitude=15):
    period = 24*60*60

    return amplitude * np.cos(2*np.pi*t / period + np.pi) + T0


def main():

    t_max = 7*24*60*60
    dt = 5
    t_N = t_max // dt


    P_eng = 1000
    U = 40
    alpha = 1
    T_ambient = 293
    C = 442*10 # c_iron * 10 Kg

    R_ref = 0.1
    T_ref = 293
    beta = 0.005

    ts = np.arange(0, t_max, dt)

    T0 = 293
    
    Ts = np.zeros(t_N)
    Ts[0] = T0

    Is = np.zeros(t_N)

    eng_on = 1

    for i, t in enumerate(ts[:-1]):

        R = R_ref*(1+beta*(Ts[i]-T_ref))

        Is[i] = (U - np.sqrt(U*U - 4*R*P_eng*eng_on)) / 2 / R

        Ts[i+1] = (Is[i]*Is[i]*R - alpha*(Ts[i] - get_ambient_temperature(t)))*dt/C + Ts[i]

        if Ts[i+1] > 450:
            eng_on = 0
        if eng_on == 0 and Ts[i+1] < 330:
            eng_on = 1

        print(Ts[i])

    plt.figure()
    plt.plot(ts, Is)

    plt.figure()
    plt.plot(ts, Ts)
    plt.show()

main()