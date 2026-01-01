import matplotlib.pyplot as plt

def rgb(r, g, b):
    return (r/255.0, g/255.0, b/255.0, 1)

GREENS = [
    rgb(232, 246, 243),
    rgb(208, 236, 231),
    rgb(162, 217, 206),
    rgb(115, 198, 182),
    rgb(69, 179, 157),
    rgb(22, 160, 133),
]

SKYS = [
    rgb(235, 245, 251),
    rgb(214, 234, 248),
    rgb(174, 214, 241),
    rgb(133, 193, 233),
    rgb(93, 173, 226),
    rgb(52, 152, 219),
]

BLUES = [
    rgb(234, 242, 248),
    rgb(212, 230, 241),
    rgb(169, 204, 227),
    rgb(127, 179, 213),
    rgb(84, 153, 199),
    rgb(41, 128, 185),
]

PURPLES = [
    rgb(244, 236, 247),
    rgb(232, 218, 239),
    rgb(210, 180, 222),
    rgb(187, 143, 206),
    rgb(165, 105, 189),
    rgb(142, 68, 173),
]

REDS = [
    rgb(253, 237, 236),
    rgb(250, 219, 216),
    rgb(245, 183, 177),
    rgb(241, 148, 138),
    rgb(236, 112, 99),
    rgb(231, 76, 60),
]

DARK_REDS = [
    rgb(249, 235, 234),
    
]

YELLOWS = [
    rgb(254, 249, 231),
    rgb(252, 243, 207),
    rgb(249, 231, 159),
    rgb(247, 220, 111),
    rgb(244, 208, 63),
    rgb(241, 196, 15),
]

ORANGES = [
    rgb(253, 242, 233),
    rgb(250, 229, 211),
    rgb(245, 203, 167),
    rgb(240, 178, 122),
    rgb(235, 152, 78),
    rgb(230, 126, 34),
]


GREYS = [
    rgb(169, 169, 169),
    rgb(169, 169, 169),
    rgb(169, 169, 169),
    rgb(169, 169, 169),
    rgb(169, 169, 169),
]


WHITES = [
    rgb(255, 255, 255),
    rgb(255, 255, 255),
    rgb(255, 255, 255),
    rgb(255, 255, 255),
    rgb(255, 255, 255),
]


BLUISH = BLUES+SKYS
REDISH = REDS+PURPLES
GREENISH = YELLOWS+GREENS
COLOR_GRADIENTS = [BLUISH, REDISH, GREENISH]
ALL_COLORS = [BLUES, ORANGES, YELLOWS, GREENS, PURPLES, REDS, SKYS]

    
''' PATTERNS '''
ALL_PATTERNS = ['', '////', '----', '||||',  'xxxx', '++++','\\\\\\\\']



''' FUNCTIONS '''
def select_color_idx(idx=2):
    color_names = "BLUE,ORANGE,YELLOW,GREEN,PURPLE,RED,SKYS".split(',')
    color_dic = {}
    for i, c in enumerate(ALL_COLORS):
        # plt.bar(i, 1, color=c[idx])
        color_dic[color_names[i]] = c[idx]
    # plt.xticks(range(i+1), color_names)
    colors = [c[idx] for c in ALL_COLORS]
    # plt.figure(figsize=(4,2))
    return colors, color_dic
    
def display_colors():
    color_names = "BLUES,ORANGES,YELLOWS,GREENS,PURPLES,REDS,SKY".split(',')
    color_dic = {}
    for i, c in enumerate(ALL_COLORS):
        color_dic[color_names[i]] = c
        for j, g in enumerate(c):
            plt.bar(i, len(c)-j, color=c[j])
    plt.xticks(range(i+1), color_names)
    plt.figure(figsize=(4,2))
    return ALL_COLORS, color_dic

'''
def display_color_grads():
    for i, c in enumerate(COLOR_GRADIENTS):
        for j, g in enumerate(c):
            plt.bar(i, len(c)-j, color=c[j])
    plt.xticks(range(i+1), "BLUISH, REDISH, GREENISH".split(','))
    return [BLUISH, REDISH, GREENISH]
'''

def display_patterns():
    print(",".join(ALL_PATTERNS))
    return ALL_PATTERNS