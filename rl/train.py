from torch.utils.tensorboard import SummaryWriter
from agents import ValueAgentDataset
from dataset import CSVDataset
import torch
from tqdm import tqdm
from parser import get_args


def main():
    args = get_args()

    dataset = CSVDataset(args.csv_file,
                         args.root_dir,
                         args.cols,
                         args.preprocess_fn,
                         args.state,
                         args.next_state,
                         args.reward,
                         args.info,
                         args.done)

    state_dim = 7
    value_net = args.network(state_dim, args.hidden_dim).to(args.device)
    target_value_net = args.network(state_dim, args.hidden_dim).to(args.device)

    optimizer = args.optimizer(value_net.parameters(), lr=args.lr)

    dataloader = torch.utils.data.DataLoader(dataset, batch_size=args.batch_size)



    agent = ValueAgentDataset(args.network,
                              dataset,
                              args.batch_size,
                              args.gamma,
                              args.device,
                              args.optimizer,
                              args.lr,
                              args.hidden_dim,
                              args.criterion,
                              args.update)

    summary = SummaryWriter(args.summary_dir)
    #summary.add_hparams()

    num_iter = 0

    for epoch in tqdm(range(args.num_epochs), desc="Epoch"):
        for idx, batch in enumerate(tqdm(dataloader, desc="Batch")):
            state, reward, next_state, info, done = batch
            #state = state.to(args.device)
            reward = reward.to(args.device)
            #next_state = next_state.to(args.device)
            k = info.to(args.device)
            done = done.to(args.device)

            value = value_net(state)
            # target_value = (reward * ((args.gamma ** k) - 1)) / (k * (args.gamma - 1) + 0.0001) + \
            #                (args.gamma ** k) * target_value_net(next_state) * (1 - done)
            target_value = reward + args.gamma * target_value_net(next_state) * (1 - done)
            value_loss = args.criterion(value, target_value.detach())

            optimizer.zero_grad()
            value_loss.backward()
            optimizer.step()

            num_iter += 1

            if num_iter % args.update == 0:
                for target_param, param in zip(target_value_net.parameters(), value_net.parameters()):
                    target_param.data.copy_(param.data)
            summary.add_scalar('Loss/Value', value_loss.detach().item(), num_iter)
            summary.add_scalar('Stats/Value Mean', value.detach().mean().item(), num_iter)
            summary.add_scalar('Stats/Value Std', value.detach().std().item(), num_iter)
    summary.close()
    print("Finished training!!!")


if __name__ == "__main__":
    main()