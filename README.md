# Grav1
Client for [grav1_ex](https://github.com/wwww-wwww/grav1_ex)

## Requirements
- Python 3.6+
- ffmpeg
- encoders (aomenc, vpxenc, etc.) if not provided by the server.

## Installation

### Linux:
```
pip3 install -r requirements.txt
```

### Windows:
```
pip install -r requirements.txt
pip install windows-curses
```

### Running
1. Copy `config.example.yaml` to `config.yaml` and modify fields:
    |  |  |
    | - | - | 
    | name | name of the client (optional) |
    | target | target server |
    | key | your api key |
    | workers | max number of workers |
    | queue | max queue size |
    | threads | number of threads per encoding worker (>= 2) |
    | aomenc | path to aomenc |
    | vpxenc | path to vpxenc |
    | ffmpeg | path to ffmpeg |

    These can also be accessed with command line arguments. Check using `python3 client.py --help`
2. Run!  
  `python3 client.py`