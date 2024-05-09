#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 22:32:27 2022

@author: ruchak
"""

import torch
from torchvision.models import resnet50, ResNet50_Weights
import torch.nn as nn
import timm
import torch.nn.functional as F
class ViTBase16(nn.Module):
    def __init__(self, pretrained = True):
        super(ViTBase16, self).__init__()
        self.model = timm.create_model('vit_base_patch16_224')
        self.model.head = nn.Linear(self.model.head.in_features, 2)
    
    def forward(self, x):
        x1 = self.model(x)
        x = F.softmax(x1, dim = 1)
        return x, x1
