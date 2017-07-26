package com.example.reactivedemo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@SpringBootApplication
public class ReactiveDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveDemoApplication.class, args);
	}

	@Bean
	RouterFunction<?> routerFunction(DataSourceHandler dh) {
		return route(GET("/datasources"), dh::all)
				.andRoute(GET("/datasources/{id}"), dh::byId)
				.andRoute(GET("/datasources/{id}/events"), dh::events);
	}
}

@Component
class DataSourceHandler {

	private final DataSourceService dataSourceService;

	DataSourceHandler(DataSourceService dataSourceService)
	{
		this.dataSourceService = dataSourceService;
	}

	Mono<ServerResponse> all(ServerRequest request) {
		return ok().body(dataSourceService.all(), DataSource.class);
	}

	Mono<ServerResponse> byId(ServerRequest request) {
		return ok().body(dataSourceService.byId(request.pathVariable("id")), DataSource.class);
	}

	Mono<ServerResponse> events(ServerRequest request) {
		return ok()
				.contentType(MediaType.TEXT_EVENT_STREAM)
				.body(dataSourceService.streamEvents(request.pathVariable("id")), DataSourceEvent.class);
	}
}

@Component
class SampleDataSources implements CommandLineRunner {

	private final DataSourceRepository dataSourceRepository;

	SampleDataSources(DataSourceRepository dataSourceRepository) {
		this.dataSourceRepository = dataSourceRepository;
	}

	@Override
	public void run(String... strings) throws Exception
	{
		this.dataSourceRepository.deleteAll().subscribe(null, null, () ->
				Stream.of("Pressure", "Temperature", "Heat", "Humidity", "Rotation")
                .forEach(dataSourceName ->
                        dataSourceRepository.save(
                                new DataSource(UUID.randomUUID().toString(), dataSourceName))
                                .subscribe(dataSource -> System.out.println(dataSource.toString()))));
	}
}

@Service
class DataSourceService {

	private DataSourceRepository dataSourceRepository;

	DataSourceService(DataSourceRepository dataSourceRepository)
	{
		this.dataSourceRepository = dataSourceRepository;
	}

	Flux<DataSourceEvent> streamEvents(String dataSourceId) {
		return byId(dataSourceId).flatMapMany(dataSource -> {
			Flux<DataSourceEvent> eventFlux = Flux.fromStream(Stream.generate(() -> new DataSourceEvent(dataSource, new Date())));
			Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
			return eventFlux.zipWith(interval).map(Tuple2::getT1);
		});
	}

	Mono<DataSource> byId(String dataSourceId) {
		return dataSourceRepository.findById(dataSourceId);
	}

	Flux<DataSource> all() {
		return dataSourceRepository.findAll();
	}
}


interface DataSourceRepository extends ReactiveMongoRepository<DataSource, String> {

}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document
class DataSource {
	@Id
	String id;
	String name;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class DataSourceEvent {
	private DataSource dataSource;
	private Date when;
}
