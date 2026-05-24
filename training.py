import torch
from model_class import OrderPredictionHeteroGNN_2
import pandas as pd
from torch_geometric.loader import DataLoader
import numpy as np


class Trainer():
    def __init__(self, noise):
        self.noise = noise
        self.train_graphs = torch.load(f"{self.noise}/HETERO_STRUCTURES/train_graphs_fg.pt", weights_only=False)
        self.val_graphs = torch.load(f"{self.noise}/HETERO_STRUCTURES/val_graphs_fg.pt", weights_only=False)

        self.train_graphs_sg = torch.load(f"{self.noise}/HETERO_STRUCTURES/train_graphs_sg.pt", weights_only=False)
        self.val_graphs_sg = torch.load(f"{self.noise}/HETERO_STRUCTURES/val_graphs_sg.pt", weights_only=False)

        self.criterion = torch.nn.L1Loss()
        self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")

        self.model_params = pd.read_csv("model_parameters.csv", delimiter=';')
        self.model_params = self.model_params[self.model_params.iloc[:, 0] == self.noise]

    def training_loop(self, model, train_loader, val_loader, viewpoint, optimizer):
        model.to(self.device)  # Move the model to the GPU (or CPU)
        model.train()

        to_div = 0
        train_loss = 0
        for idx, batch in enumerate(train_loader):
            # Move the batch to the appropriate device
            batch = batch.to(self.device)

            optimizer.zero_grad()

            out = model(batch)
            mask = batch[viewpoint].mask
            loss = self.criterion(out[mask], batch.y_dict[viewpoint][mask])

            loss.backward()
            optimizer.step()

            to_div += sum(mask)
            train_loss += loss.item() * sum(mask)

        test_loss = 0
        to_div_2 = 0

        with torch.no_grad():
            model.eval()
            for idx, batch in enumerate(val_loader):
                batch = batch.to(self.device)
                # Move the batch to the appropriate device

                out = model(batch)
                mask = batch[viewpoint].mask
                loss = self.criterion(out[mask], batch.y_dict[viewpoint][mask])

                to_div_2 += sum(mask)
                test_loss += loss.item() * sum(mask)

        return (train_loss / to_div).item(), (test_loss / to_div_2).item()

    def trainer(self, TASK):
        flag = True
        viewpoint = TASK.lower()
        index = ['ORDER', 'ITEM', 'PACKAGE'].index(TASK)
        num_layers = self.model_params.iloc[index, 2]
        width_layers = self.model_params.iloc[index, 3]
        heads = self.model_params.iloc[index, 4]

        batch_size = int(self.model_params.iloc[index, 5])
        epochs_fg = self.model_params.iloc[index, 6]
        epochs_sg = self.model_params.iloc[index, 7]

        learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
        PATIENCE = 5

        train_loader = DataLoader(self.train_graphs, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(self.val_graphs, batch_size=len(self.val_graphs))

        avg_ord_train = np.mean([len(batch[viewpoint]['x']) for batch in train_loader])
        print(avg_ord_train)

        train_loader_sg = DataLoader(self.train_graphs_sg, batch_size=int(avg_ord_train), shuffle=True)
        val_loader_sg = DataLoader(self.val_graphs_sg, batch_size=len(self.val_graphs_sg))

        to_train = [i for i in range(1, 6)]
        while flag:
            for i in to_train:
                model_fg = OrderPredictionHeteroGNN_2([width_layers] * num_layers, 1, num_layers, heads, viewpoint)
                min_loss = 10e7
                counter = 0

                for epoch, lr in enumerate(learning_rates):
                    optimizer = torch.optim.Adam(model_fg.parameters(), lr=lr)
                    temp = self.training_loop(model_fg, train_loader, val_loader, viewpoint, optimizer)
                    if temp[1] < min_loss:
                        min_loss = temp[1]
                        torch.save(model_fg.state_dict(), f"{self.noise}/MODELS/{TASK}/GAT_fg_{i}.pth")
                        counter = 0
                    else:
                        counter += 1

                    if counter > PATIENCE:
                        print('---')
                        break
                if epoch + 1 >= epochs_fg:
                    to_train.remove(i)
            if len(to_train) == 0:
                flag = False

        to_train = [i for i in range(1, 6)]
        while flag:
            for i in to_train:
                model_sg = OrderPredictionHeteroGNN_2([width_layers] * num_layers, 1, num_layers, heads, viewpoint)
                min_loss = 10e7
                counter = 0

                for epoch, lr in enumerate(learning_rates):
                    optimizer = torch.optim.Adam(model_sg.parameters(), lr=lr)
                    temp = self.training_loop(model_sg, train_loader_sg, val_loader_sg, viewpoint, optimizer)

                    if temp[1] < min_loss:
                        min_loss = temp[1]
                        torch.save(model_sg.state_dict(), f"{self.noise}/MODELS/{TASK}/GAT_sg_{i}.pth")
                        counter = 0
                    else:
                        counter += 1

                    if counter > PATIENCE:
                        print('---')
                        break
                if epoch + 1 >= epochs_sg:
                    to_train.remove(i)
            if len(to_train) == 0:
                flag = False



