import os

def func(name: str):
    d = f'build/{name}'
    for file_name in os.listdir(d):
        k = file_name.split('.')[0]
        with open(os.path.join(d, file_name)) as file:
            val = file.readline()
        print(name, f'{k=}', val, end='')

if __name__ == '__main__':
    func('clusterizationKmeans')
    func('clusterizationGaussianMixture')
