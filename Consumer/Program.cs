using Consumer;
using Consumer.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<PeopleDbContext>(x => 
			    x.UseSqlServer("Data Source=.\\V19;Initial Catalog=PeopleDb;User ID=sa;Password=1;Trusted_Connection=True;MultipleActiveResultSets=true;Encrypt=false"));

builder.Services.AddHostedService<PeopleBackgroundService>();

var app = builder.Build();

await app.RunAsync();