import torch
from torch_geometric.loader import DataLoader
from model_class import OrderPredictionHeteroGNN_2
import pandas as pd
import warnings
import json
import sup_funcs as sf

warnings.filterwarnings("ignore")


class Evaluation:
    def __init__(self, database, cant):
        self.criterion = torch.nn.L1Loss()
        self.device = torch.device('cpu')
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()

        self.model_params = pd.read_csv("files/model_parameters.csv", delimiter=',')
        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json") as json_file:
            self.tensor_dict = json.load(json_file)

    @torch.no_grad()
    def acc_eval(self, model, loader, kpi_ob):
        model.to(self.device)
        model.eval()

        total_examples = total_right = 0
        for idx, batch in enumerate(loader):
            batch = batch.to(self.device)
            # Move the batch to the appropriate device

            out = model(batch).argmax(dim=-1)
            mask = batch[kpi_ob].mask
            correct = (out[mask] == batch.y_dict[kpi_ob][mask]).sum()
            total_examples += sum(mask)
            total_right += correct
        return (total_right / total_examples).item()

    def eval_model(self, model, loader, kpi_ob):
        model.to(self.device)
        model.eval()

        test_loss = 0
        to_div_2 = 0

        with torch.no_grad():
            for idx, batch in enumerate(loader):
                batch = batch.to(self.device)
                out = model(batch)
                mask = batch[kpi_ob].mask
                loss = self.criterion(out[mask], batch.y_dict[kpi_ob][mask])

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
        obs = self.path_dict['kpis'][self.path_dict['kpi_event']]
        print("Loading models and data...")
        all_results = {v: [[], []] for v in obs}

        test_graphs_sg = torch.load(f"{self.path_dict['hetero_path']}/test_graphs_sg.pt", weights_only=False)
        test_loader_sg = DataLoader(test_graphs_sg, batch_size=len(test_graphs_sg))

        for kpi_ob in obs:
            parameters = self.model_params[self.model_params['kpi_ob'] == kpi_ob]
            num_layers = int(parameters['num_layers'].values[0])
            width_layers = int(parameters['width_layers'].values[0])
            heads = int(parameters['heads'].values[0])

            model_path = f"{self.path_dict['model_output_path']}/{kpi_ob}"

            if self.path_dict['kpi_type'] == 0:
                num_variables = 1
            elif self.path_dict['kpi_type'] == 1:
                num_variables = 2
            elif self.path_dict['kpi_type'] == 2:
                num_variables = 4

            for i in range(1, 6):
                model_sg = OrderPredictionHeteroGNN_2([width_layers] * num_layers, num_variables, num_layers,
                                                      heads, self.tensor_dict, kpi_ob)
                state_dict = torch.load(f"{model_path}/GAT_sg_{i}.pth", weights_only=False)
                model_sg.load_state_dict(state_dict)

                if self.path_dict['kpi_type'] == 0:
                    loss_sg = round(self.eval_model(model_sg, test_loader_sg, kpi_ob), 5)
                elif self.path_dict['kpi_type'] == 1 or self.path_dict['kpi_type'] == 2:
                    loss_sg = round(self.acc_eval(model_sg, test_loader_sg, kpi_ob), 5)

                print(f"{i} - {kpi_ob} - Loss SG : {loss_sg}")

                all_results[kpi_ob][1].append(loss_sg)
        print("Done! \n", all_results)

