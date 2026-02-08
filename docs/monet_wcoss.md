# Installing on WCOSS

If you have access to the NOAA WCOSS machines, you can create your own Python environments very easily using Intel Python.

```bash
module load ips/19.0.5.281
```

Then it is suggested to create a `.condarc` file that lives in your `$HOME` folder to point to a location that will house your Conda environments. Below is a sample `.condarc` file.

```yaml
channels:
  - intel
  - conda-forge
  - defaults
envs_dirs:
  - /gpfs/dell2/emc/verification/noscrub/User.Name/conda/envs
pkgs_dirs:
  - /gpfs/dell2/emc/verification/noscrub/User.Name/conda/pkgs
```

Next, you should start a new environment by cloning the default environment to a new name:

```bash
conda create -n myenv --clone="/usrx/local/prod/intel/2019UP05/intelpython3"
```

Activate the environment:

```bash
source activate myenv
```

From here, you can install MONET via conda:

```bash
conda install -c conda-forge monet
```
