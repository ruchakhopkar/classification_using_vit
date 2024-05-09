#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 18:25:39 2022

@author: ruchak
"""
import os
import random
import torch
import numpy as np
import matplotlib.pyplot as plt
from datasets import CreateDataset, CreateDatasetTest
from torch.utils.data import DataLoader
import shap
from models import *
import torch.nn as nn
import torch.optim as optim
from torch.optim.lr_scheduler import ReduceLROnPlateau
from tqdm import tqdm


path_test = '/home/ruchak/peg_classification/PFA/1.abnormal/'
path = '/home/ruchak/peg_classification/PFA/scripts/'
batch_size = 64
lr = 1e-5
train = False
epochs = 1
gamma = 0.65
early_stopping = 5
seed = 42

def seed_everything(seed):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True

seed_everything(seed)

# train_data = CreateDataset(path + 'images/', path + 'df_train.csv')
# val_data = CreateDataset(path+ 'images/', path + 'df_val.csv')
test_data = CreateDatasetTest(path_test, path + 'df_test.csv')
#test_data = CreateDatasetTest(path + 'images/', path + 'data.csv')
# train_loader = DataLoader(dataset = train_data, batch_size=batch_size, shuffle=True)
# val_loader = DataLoader(dataset = val_data, batch_size=batch_size, shuffle=False)
test_loader = DataLoader(dataset = test_data, batch_size=batch_size, shuffle=False)

device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = ViTBase16()
model = model.to(device)
criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr = lr)
if train == False:
    checkpoint = torch.load(path + 'model_checkpoint.pt')
    model.load_state_dict(checkpoint['model_state_dict'])
    optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
scheduler = ReduceLROnPlateau(optimizer, patience = 3, factor = 0.5, threshold = 0.001)

train_loss, train_acc, val_acc, val_loss = [], [], [], []
predictions = []
best_acc = 0.0
early_stop = 0
if train:
    for epoch in range(epochs):
        print(f"====================== EPOCH {epoch+1} ======================")
        print("Training.....")
        model.train()
        running_loss = 0.0
        running_corrects = 0
        for data, label in tqdm(train_loader):
            
            data = data.to(device)
            label = label.to(device).float()
            output = model(data)
            _, preds = torch.max(output, 1)
            loss = criterion(output, label)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
            running_corrects += torch.sum(preds == torch.max(label,1)[1]).item()/len(data)
        train_loss.append(running_loss/(len(train_loader)))
        train_acc.append(running_corrects/(len(train_loader)))
        print("Validation.....")
        
        model.eval()
        running_loss = 0
        running_corrects = 0
        predictions = []
        with torch.no_grad():
    
            for data, label in val_loader:
                data = data.to(device)
                label = label.to(device).float()
    
                val_output = model(data)
                _, preds = torch.max(val_output, 1)
                predictions.append(val_output.cpu())
                loss = criterion(val_output, label)
                running_loss += loss.item()
                running_corrects += torch.sum(preds == torch.max(label, 1)[1]).item()/len(data)
            val_loss.append(running_loss/len(val_loader))
            val_acc.append(running_corrects/len(val_loader))
        predictions = np.concatenate(predictions, axis = 0)
        np.save(path+'predictions_all.npy',predictions)
    
    
    
        scheduler.step(running_loss/len(val_loader))
                
        print("====================================================")
        print(f"TRAIN ACC : {train_acc[-1]}  TRAIN LOSS : {train_loss[-1]}")
        print(f"VALL ACC : {val_acc[-1]}  VAL LOSS : {val_loss[-1]}")
        print("====================================================")
        if val_acc[-1]>best_acc:
            best_acc = val_acc[-1]
            torch.save({
                   'epoch': epoch,
                   'model_state_dict': model.state_dict(),
                   'optimizer_state_dict': optimizer.state_dict(),
                   'loss': val_loss[-1],
                   'acc' : val_acc[-1]
                   }, path+'model_checkpoint.pt')   
            np.save(path+'predictions.npy', predictions)
        if (epoch>2):
            if(val_loss[-1]> val_loss[-2]):
                early_stop += 1
            else:
                early_stop = 0
        if early_stop == early_stopping:
            print(f'Early stopping at epoch{epoch}')
            break
    
    plt.plot(train_acc, label="train")
    plt.plot(val_acc, label="val")
    plt.legend()
    plt.title('ACCURACY VS EPOCH')
    plt.savefig(path + 'accuracy.png')
    plt.show()
        
    plt.plot(train_loss, label="train")
    plt.plot(val_loss, label="val")
    plt.legend()
    plt.title('LOSS VS EPOCH')
    plt.savefig(path + 'plot.png')
    plt.show()
else:
        model.eval()
        
        predictions = []
        pre_softmax = []
        with torch.no_grad():
    
            for data in test_loader:
                data = data.to(device)
                
    
                val_output, x1 = model(data)
                _, preds = torch.max(val_output, 1)
                predictions.append(val_output.cpu())
                pre_softmax.append(x1.cpu())
                
        predictions = np.concatenate(predictions, axis = 0)
        np.save(path + 'predictions_test.npy',predictions)
        
        pre_softmax = np.concatenate(pre_softmax, axis = 0)
        np.save(path + 'predictions_pre_softmax.npy', pre_softmax)

        # batch = next(iter(test_loader))
        
        # images = batch[0]
        # background = images[:50].to(device)
        # test_images = images[4:9].to(device)


        # e = shap.DeepExplainer(model, background)
        # shap_values = e.shap_values(test_images)

        # shap_numpy = [np.swapaxes(np.swapaxes(s, 1, -1), 1, 2) for s in shap_values]
        # test_numpy = np.swapaxes(np.swapaxes(test_images.cpu().numpy(), 1, -1), 1, 2)
        # print(shap_numpy, test_numpy)
        # shap.image_plot(shap_numpy, -test_numpy)
        # plt.savefig(path + 'final_shap.png')

