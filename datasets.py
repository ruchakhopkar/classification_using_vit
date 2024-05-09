#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 21:57:58 2022

@author: ruchak
"""

import torch
from torch.utils.data import DataLoader, Dataset
import pandas as pd
import numpy as np
from PIL import Image
from torchvision import datasets, transforms

class CreateDataset(Dataset):
    def __init__(self, directory, dataset):
        self.df_train = pd.read_csv(dataset)   
        self.directory = directory
        self.transforms = transforms.Compose(
            [
                transforms.Resize((224, 224)),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                transforms.ConvertImageDtype(torch.float32),
                transforms.Normalize(mean = [0.485, 0.456, 0.406], std = [0.229, 0.224, 0.225])
            ])


    def __len__(self):
        return len(self.df_train)

    def __getitem__(self, idx):
        row = self.df_train.iloc[idx]
        img = Image.open(self.directory + row['HEAD_SERIAL_NUM']+ '.png').convert('RGB')
        img = self.transforms(img)
        y = torch.LongTensor(self.df_train[['Type']].iloc[idx])
        return img, y
            
class CreateDatasetTest(Dataset):
    def __init__(self, directory, dataset):
        self.df_train = pd.read_csv(dataset)
        self.directory = directory
        self.transforms = transforms.Compose(
            [
                transforms.Resize((224, 224)),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                transforms.ConvertImageDtype(torch.float32),
                transforms.Normalize(mean = [0.485, 0.456, 0.406], std = [0.229, 0.224, 0.225])
            ])


    def __len__(self):
        return len(self.df_train)

    def __getitem__(self, idx):
        row = self.df_train.iloc[idx]
        img = Image.open(self.directory + row['HEAD_SERIAL_NUM']+ '.png')
        img = self.transforms(img)
        return img

