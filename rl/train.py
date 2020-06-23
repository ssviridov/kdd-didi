from torch.utils.tensorboard import SummaryWriter
from agents import ValueAgentDataset
from dataset import CSVDataset
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
    for epoch in range(args.num_epochs):

        value_loss, value_mean, value_std = agent.train()
        summary.add_scalar('Loss/Value', value_loss, num_iter)
        summary.add_scalar('Stats/Value Mean', value_mean, num_iter)
        summary.add_scalar('Stats/Value Std', value_std, num_iter)
        num_iter += 1
    summary.close()


if __name__ == "__main__":
    main()
