using Microsoft.EntityFrameworkCore;

namespace Consumer.Models;

public class PeopleDbContext(DbContextOptions<PeopleDbContext> options) : DbContext(options)
{
	public DbSet<Person> People { get; set; } = default!;
}