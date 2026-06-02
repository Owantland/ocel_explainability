import torch
from torch_geometric.loader import DataLoader
from model_class import OrderPredictionHeteroGNN_2
import pandas as pd
import warnings
import json
import sup_funcs as sf

warnings.filterwarnings("ignore")


class Evaluation:
    def __init__(self, database):
        self.criterion = torch.nn.L1Loss()
        self.device = torch.device('cpu')
        self.funcs = sf.SupportFunctions(database)
        self.path_dict = self.funcs.get_paths()

        self.model_params = pd.read_csv("files/model_parameters.csv", delimiter=',')
        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json") as json_file:
            self.tensor_dict = json.load(json_file)

    def eval_model(self, model, viewpoint, loader):
        model.to(self.device)
        model.eval()

        test_loss = 0
        to_div_2 = 0

        with torch.no_grad():
            for idx, batch in enumerate(loader):
                batch = batch.to(self.device)
                out = model(batch)
                mask = batch[viewpoint].mask
                loss = self.criterion(out[mask], batch.y_dict[viewpoint][mask])

                to_div_2 += sum(mask)
                test_loss += loss.item() * sum(mask)

        return (test_loss / to_div_2).item()

    def eval_model_package(self, model, loader):
        model.to(self.device)
        model.eval()

        test_loss = 0
        to_div_2 = 0

        with torch.no_grad():
            for idx, batch in enumerate(loader):
                orders = []
                batch = batch.to(self.device)
                idx = -1
                for package_idx in range(len(batch['package']['x'])):
                    items = batch[('package', 'to', 'item')]['edge_index'][1][
                        batch[('package', 'to', 'item')]['edge_index'][0] == package_idx]
                    mask = torch.isin(batch[('order', 'to', 'item')]['edge_index'][1], items)
                    result = torch.unique(batch[('order', 'to', 'item')]['edge_index'][0][mask])
                    orders.append(len(result))

                mask = batch['package'].mask
                out = model(batch)[mask]
                y = batch.y_dict['package'][mask]
                out_2 = []
                y_2 = []
                somma = 0
                out_idx = 0
                for idx, item in enumerate(mask):
                    if item:
                        out_2.extend([out[out_idx]] * orders[idx])
                        y_2.extend([y[out_idx]] * orders[idx])
                        out_idx += 1
                        somma += orders[idx]

                loss = self.criterion(torch.tensor(out_2), torch.tensor(y_2))

        return loss.item()

    def evalutaion(self):
        # obs = self.path_dict['kpis'][self.path_dict['kpi_event']]
        obs = ['Orders']
        print("Loading models and data...")
        all_results = {}
        all_results = {v: [[], []] for v in obs}

        test_graphs_sg = torch.load(f"{self.path_dict['hetero_path']}/test_graphs_sg.pt", weights_only=False)
        test_loader_sg = DataLoader(test_graphs_sg, batch_size=len(test_graphs_sg))

        for index, kpi_ob in enumerate(obs):
            num_layers = self.model_params.iloc[index, 1]
            width_layers = self.model_params.iloc[index, 2]
            heads = self.model_params.iloc[index, 3]
            model_path = f"{self.path_dict['model_output_path']}/{kpi_ob}"

            for i in range(1, 6):
                model_sg = OrderPredictionHeteroGNN_2([width_layers] * num_layers, 1, num_layers,
                                                      heads, self.tensor_dict, kpi_ob)
                state_dict = torch.load(f"{model_path}/GAT_sg_{i}.pth", weights_only=False)
                model_sg.load_state_dict(state_dict)

                if kpi_ob == 'package':
                    loss_sg = round(self.eval_model_package(model_sg, test_loader_sg), 5)
                else:
                    loss_sg = round(self.eval_model(model_sg, kpi_ob, test_loader_sg), 5)

                print(f"{i} - {kpi_ob} - Loss SG : {loss_sg}")

                all_results[kpi_ob][1].append(loss_sg)
        print("Done! \n", all_results)

